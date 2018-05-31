#!/usr/bin/env ruby
#   Usage: <progname> [options]...
#   options
#    -h --help  display help
#    -d --debug run in debug mode
#    -s --start <timestamp> retrieve sub-set defined by modified timestamp
#    -r --reprocess reprocess a commandline specified file of IDs, one per line to process
require 'swift_ingest'
require 'optparse'
require 'logger'
require 'time'
require_relative 'cwrc_common'

module CWRCPerserver
  # process command line arguments
  debug_level = false
  start_dt = ''
  reprocess = ''

  file = __FILE__
  ARGV.options do |opts|
    opts.on('-d', '--debug')             { debug_level = true }
    opts.on('-s', '--start=val', String) { |val| start_dt = val }
    opts.on_tail('-h', '--help')         { exec "grep ^#[[:space:]]<'#{file}'|cut -c6-" }
    opts.on('-r', '--reprocess=val', String) { |val| reprocess = val }
    opts.parse!
  end

  # set environment
  set_env

  # load exception files
  except_file = ENV['SWIFT_ARCHIVE_FAILED']
  success_file = ENV['SWIFT_ARCHIVED_OK']

  # setup logger and log level
  log = Logger.new(STDOUT)
  log.level = Logger::DEBUG if debug_level
  log.debug("Retrieving all objects modified since: #{start_dt}")

  # get connection cookie
  cookie = retrieve_cookie
  log.debug("Using connecion cookie: #{cookie}")

  # connect to swift storage
  # swift_depositer = connect_to_swift
  # raise CWRCArchivingError if swift_depositer.nil?

  # get list of all objects from cwrc
  cwrc_objs = if !reprocess.to_s.empty?
                rp_list = []
                log.debug("Processing by file: #{reprocess}")
                File.open(reprocess).each { |line| rp_list << line } if File.exist?(reprocess)
                rp_list.collect { |v| { 'pid' => v.strip } }
              else
                log.debug('Processing api response')
                get_cwrc_objs(cookie, start_dt)
              end
  log.debug("Number of objects to precess: #{cwrc_objs&.length}")

  # for each cwrc object
  cwrc_objs&.each do |cwrc_obj|
    cwrc_file_str = cwrc_obj['pid'].to_s
    cwrc_file = "#{cwrc_file_str.tr(':', '_')}.zip"

    log.debug("PROCESSING OBJECT: #{cwrc_file_str}, modified timestamp #{cwrc_obj['timestamp']}")

    # check if file has been deposited, handle open stack bug causing exception in openstack/connection
    force_deposit = false || !reprocess.to_s.empty?
    begin
      swift_file = swift_depositer.get_file_from_swit(cwrc_file, ENV['CWRC_SWIFT_CONTAINER']) unless force_deposit
    rescue StandardError
      force_deposit = true
    end

    # if object is not is swift or we have newer object
    next unless force_deposit ||
                swift_file.nil? ||
                swift_file.bytes.to_f.zero? ||
                swift_file.metadata['timestamp'].nil? ||
                cwrc_obj['timestamp'].to_s.to_time > swift_file.metadata['timestamp'].to_s.to_time

    # download object from cwrc
    start_time = Time.now
    log.debug("DOWNLOADING: #{cwrc_file}")
    begin
      download_cwrc_obj(cookie, cwrc_obj, cwrc_file)
    rescue Net::ReadTimeout,
           Net::HTTPBadResponse,
           Net::HTTPHeaderSyntaxError,
           Net::HTTPServerError,
           Net::HTTPError,
           Errno::ECONNRESET,
           Errno::EHOSTUNREACH,
           Errno::EINVAL,
           EOFError => e
      log.error("ERROR DOWNLOADING: #{cwrc_file} - #{e.class} #{e.message} #{e.backtrace}")
      next
    end
    file_size = File.size(cwrc_file).to_f / 2**20
    fs_str = format('%.3f', file_size)
    log.debug("SIZE: #{fs_str} MB")
    cwrc_time = Time.now

    # deposit into swift an remove file, handle swift errors
    begin
      swift_depositer.deposit_file(cwrc_file, ENV['CWRC_SWIFT_CONTAINER'], last_mod_timestamp: cwrc_obj['timestamp'])
    rescue StandardError => e
      log.error("SWIFT DEPOSITING ERROR #{e.message}")
      File.open(except_file, 'a') { |err_file| err_file.write("#{cwrc_file_str}\n") }
      FileUtils.rm_rf(cwrc_file) if File.exist?(cwrc_file)
      next
    end
    swift_time = Time.now

    # cleanup - remove file and print statistics
    FileUtils.rm_rf(cwrc_file) if File.exist?(cwrc_file)
    dp_rate = format('%.3f', (file_size / (swift_time - start_time)))
    cwrc_rate = format('%.3f', (file_size / (cwrc_time - start_time)))
    swift_rate = format('%.3f', (file_size / (swift_time - cwrc_time)))
    log.debug("FILE DEPOSITED: #{cwrc_file}, deposit rate #{dp_rate} (#{cwrc_rate} #{swift_rate}) MB/sec")
    File.open(success_file, 'a') do |ok_file|
      ok_file.write("#{cwrc_file_str} #{fs_str} MB #{dp_rate} (#{cwrc_rate} #{swift_rate}) MB/sec\n")
    end
  end
end
