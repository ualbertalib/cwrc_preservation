#!/usr/bin/env ruby
# Query the CWRC repository for object to preserve within a OpenStack
# Swift preservation stack
#
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

  ARGV.options do |opts|
    opts.banner = 'Usage: cwrc_preserver [options]'
    opts.separator ''
    opts.separator 'options:'

    opts.on('-d', '--debug', 'set log level to debug') do ||
      debug_level = true
    end

    opts.on('-r', '--reprocess=path', String,
            'path to file contain IDs, one per line, for processing') do |val|
      reprocess = val
    end

    opts.on('-s', '--start=val', String,
            'subset of material modified after specified ISO-8601 date/time') do |val|
      start_dt = val
    end

    opts.on_tail('-h', '--help') do
      puts opts
      exit
    end
    opts.parse!
  end

  # set environment
  set_env

  # load exception files
  log_dir = ENV['CWRC_PRESERVER_LOG_DIR']
  time_str = Time.now.strftime('%Y-%m-%d_%H-%M-%S')
  except_file = File.join(log_dir, time_str + '_' + ENV['SWIFT_ARCHIVE_FAILED'])
  success_file = File.join(log_dir, time_str + '_' + ENV['SWIFT_ARCHIVED_OK'])
  Dir.mkdir(log_dir) unless File.exist?(log_dir)

  # working directory
  work_dir = ENV['CWRC_PRESERVER_WORK_DIR']
  Dir.mkdir(work_dir) unless File.exist?(work_dir)

  # setup logger and log level
  log = Logger.new(STDOUT)
  log.level = debug_level ? Logger::DEBUG : Logger::INFO
  log.debug("Retrieving all objects modified since: #{start_dt}")

  # get connection cookie
  cookie = retrieve_cookie
  log.debug("Using connecion cookie: #{cookie}")

  # connect to swift storage
  swift_depositer = connect_to_swift
  raise CWRCArchivingError if swift_depositer.nil?

  # get list of all objects from cwrc
  cwrc_objs = if !reprocess.to_s.empty?
                rp_list = []
                log.debug("Processing ID list from file: #{reprocess}")
                File.open(reprocess).each { |line| rp_list << line } if File.exist?(reprocess)
                rp_list.collect { |v| { 'pid' => v.strip } }
              else
                log.debug('Processing api response')
                get_cwrc_objs(cookie, start_dt)
              end
  log.debug("Number of objects to process: #{cwrc_objs.nil? ? '0' : cwrc_objs&.length}")

  # for each cwrc object
  cwrc_objs&.each do |cwrc_obj|
    cwrc_file_str = cwrc_obj['pid'].to_s
    cwrc_file = "#{cwrc_file_str.tr(':', '_')}.zip"
    cwrc_file_tmp_path = File.join(work_dir, cwrc_file)

    log.debug("PROCESSING OBJECT: #{cwrc_obj['pid']}, modified timestamp #{cwrc_obj['timestamp']}")

    # check if file has been deposited, handle open stack bug causing exception in openstack/connection
    force_deposit = false || !reprocess.to_s.empty?
    begin
      swift_file = swift_depositer.get_file_from_swit(cwrc_file, ENV['CWRC_SWIFT_CONTAINER']) unless force_deposit
    rescue StandardError => e
      force_deposit = true
      log.debug("Force deposit in swift: #{cwrc_obj['pid']} #{e.message}")
    end

    # if object is not is swift or we have newer object
    next unless force_deposit ||
                swift_file.nil? ||
                swift_file.bytes.to_f.zero? ||
                swift_file.metadata['timestamp'].nil? ||
                cwrc_obj['timestamp'].to_s.to_time > swift_file.metadata['timestamp'].to_s.to_time

    # download object from cwrc
    start_time = Time.now
    log.debug("DOWNLOADING from CWRC: #{cwrc_obj['pid']}")
    begin
      download_cwrc_obj(cookie, cwrc_obj, cwrc_file_tmp_path)
    rescue Net::ReadTimeout,
           Net::HTTPBadResponse,
           Net::HTTPHeaderSyntaxError,
           Net::HTTPServerError,
           Net::HTTPError,
           Errno::ECONNRESET,
           Errno::EHOSTUNREACH,
           Errno::EINVAL,
           EOFError => e
      log.error("ERROR DOWNLOADING: #{cwrc_obj['pid']} - #{e.class} #{e.message} #{e.backtrace}")
      next
    end

    file_size = File.size(cwrc_file_tmp_path).to_f / 2**20
    fs_str = format('%.3f', file_size)
    log.debug("SIZE: #{fs_str} MB")
    cwrc_time = Time.now

    # deposit into swift and remove file, handle swift errors
    begin
      swift_depositer.deposit_file(cwrc_file_tmp_path,
                                   ENV['CWRC_SWIFT_CONTAINER'],
                                   last_mod_timestamp: cwrc_obj['timestamp'])
    rescue StandardError => e
      log.error("SWIFT DEPOSITING ERROR #{e.message}")
      File.open(except_file, 'a') { |err_file| err_file.write("#{cwrc_obj['pid']}\n") }
      FileUtils.rm_rf(cwrc_file_tmp_path) if File.exist?(cwrc_file_tmp_path)
      next
    end
    swift_time = Time.now

    # cleanup - remove file
    FileUtils.rm_rf(cwrc_file_tmp_path) if File.exist?(cwrc_file_tmp_path)

    # print statistics
    dp_rate = format('%.3f', (file_size / (swift_time - start_time)))
    cwrc_rate = format('%.3f', (file_size / (cwrc_time - start_time)))
    swift_rate = format('%.3f', (file_size / (swift_time - cwrc_time)))
    log.debug("FILE DEPOSITED: #{cwrc_file}, deposit rate #{dp_rate} (#{cwrc_rate} #{swift_rate}) MB/sec")
    File.open(success_file, 'a') do |ok_file|
      ok_file.write("#{cwrc_obj['pid']} #{fs_str} MB #{dp_rate} (#{cwrc_rate} #{swift_rate}) MB/sec\n")
    end
  end
end
