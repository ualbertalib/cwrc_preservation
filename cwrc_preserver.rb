#!/usr/bin/env ruby
#   Usage: <progname> [options]...
#   options
#    -h --help  display help
#    -d --debug run in debug mode
#    -s --start <timestamp> retrieve sub-set defined by modified timestamp
require 'swift_ingest'
require 'optparse'
require 'logger'
require 'time'
require_relative 'cwrc_common'

module CWRCPerserver
  # process command line arguments
  debug_level = false
  start_dt = ''

  file = __FILE__
  ARGV.options do |opts|
    opts.on('-d', '--debug')             { debug_level = true }
    opts.on('-s', '--start=val', String) { |val| start_dt = val }
    opts.on_tail('-h', '--help')         { exec "grep ^#[[:space:]]<'#{file}'|cut -c5-" }
    opts.parse!
  end

  # load exception files
  except_file = 'swift_failed.txt'
  except_list = Array.new
  File.open(except_file).each { |line| except_list << line } if File.exist?(except_file)

  # setup logger and log level
  log = Logger.new(STDOUT)
  log.level = Logger::DEBUG if debug_level
  log.debug("Retrieving all objects modified since: #{start_dt}")

  # set environment
  set_env
  http_read_timeout = ENV['CWRC_READ_TIMEOUT'].to_i

  # get connection cookie
  cookie = retrieve_cookie
  log.debug("Using connecion cookie: #{cookie}")

  # connect to swift storage
  swift_depositer = connect_to_swift
  raise CWRCArchivingError if swift_depositer.nil?

  # get list of all objects from cwrc
  cwrc_objs = get_cwrc_objs(cookie, start_dt)
  log.debug("Number of objects to precess: #{cwrc_objs.length}")

  # for each cwrc object
  cwrc_objs.each do |cwrc_obj|
    cwrc_file_str = "#{cwrc_obj['pid'].to_s}"
    cwrc_file = "#{cwrc_file_str.tr(':', '_')}.zip"

    # if obj in exception list skip it
    next if except_list.include?(cwrc_file_str)

    log.debug("PROCESSING OBJECT: #{cwrc_file_str}, modified timestamp #{cwrc_obj['timestamp']}")
    start_time = Time.now

    # check if file has been deposited, handle open stack bug causing exception in openstack/connection 
    force_deposit = false
    begin
      swift_file = swift_depositer.get_file_from_swit(cwrc_file, ENV['CWRC_SWIFT_CONTAINER'])
    rescue => e
      force_deposit = true
    end

    # if object is not is swift or we have newer object
    next unless force_deposit || swift_file.nil? || cwrc_obj['timestamp'].to_s.to_time > swift_file.metadata['timestamp'].to_s.to_time

    # download object from cwrc
    log.debug("DOWNLOADING: #{cwrc_file}")
    begin
      download_cwrc_obj(cookie, cwrc_obj, cwrc_file)
    rescue Net::ReadTimeout
      log.error("ERROR DOWNLOADING: #{cwrc_file}")
      next
    end
    file_size = File.size(cwrc_file)
    log.debug("SIZE: #{format('%.2f', (file_size.to_f / 2**20))} MB")

    # deposit into swift an remove file, handle swift errors
    begin
      swift_depositer.deposit_file(cwrc_file, ENV['CWRC_SWIFT_CONTAINER'], timestamp: cwrc_obj['timestamp'])
    rescue => e
      log.error("SWIFT DEPOSITING ERROR #{e.message}")
      File.open('swift_failed.txt', 'a') { |file| file.write("#{cwrc_file_str}\n") }  # save obj name to file
      FileUtils.rm_rf(cwrc_file) if File.exist?(cwrc_file)
      next
    end

    # cleanup - remove file and print statistics
    FileUtils.rm_rf(cwrc_file) if File.exist?(cwrc_file)
    deposit_rate = format('%.2f', ((file_size.to_f / 2**20) / (Time.now - start_time)))
    log.debug("FILE DEPOSITED: #{cwrc_file}, deposit rate #{deposit_rate} MB/sec")
  end
end
