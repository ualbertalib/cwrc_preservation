#!/usr/bin/env ruby
# Query the CWRC repository for object to preserve within a OpenStack
# Swift preservation stack
#
#   Usage: <progname> [options]...
#   options
#    -C, --config PATH     Path for YAML config file
#    -d, --debug           set log level to debug
#    -r, --reprocess=path  process subset of material: path to file containing IDs, one per line
#    -s, --start=val       process subset of material: objects modified after specified ISO-8601 YYY-MM-DD <timestamp>
#    -h, --help

require 'swift_ingest'
require 'optparse'
require 'logger'
require 'time'
require_relative 'cwrc_common'

module CWRCPreserver
  # process command line arguments
  debug_level = false
  start_dt = ''
  reprocess = ''
  config_file = './secrets.yml'

  ARGV.options do |opts|
    opts.banner = 'Usage: cwrc_preserver [options]'
    opts.separator ''
    opts.separator 'options:'

    opts.on '-C', '--config PATH', 'Path for YAML config file' do |val|
      config_file = val
    end

    opts.on('-d', '--debug', 'set log level to debug') do
      debug_level = true
    end

    opts.on('-r', '--reprocess=path', String,
            'process subset of material: path to file containing IDs, one per line') do |val|
      reprocess = val
    end

    opts.on('-s', '--start=val', String,
            'process subset of material: objects modified after specified ISO-8601 YYY-MM-DD <timestamp>') do |val|
      start_dt = val
    end

    opts.on_tail('-h', '--help') do
      puts opts
      exit
    end
    opts.parse!
  end

  # set environment
  init_env(config_file)

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
  log.level = if debug_level || ENV['DEBUG'] == 'true'
                Logger::DEBUG
              else
                Logger::INFO
              end
  log.debug("Retrieving all objects modified since: #{start_dt}") unless start_dt.nil?

  # get connection cookie
  cookie = retrieve_cookie
  log.debug("Using connecion cookie: #{cookie}")

  # connect to swift storage
  swift_depositer = connect_to_swift
  raise CWRCArchivingError if swift_depositer.nil?

  # get list of all objects from cwrc
  # returns an array of objects (e.g., [{"pid"=>"1"}, {"pid"=>"2"}])
  cwrc_objs = if !reprocess.empty?
                rp_list = []
                log.debug("Processing ID list from file: #{reprocess}")
                File.open(reprocess).each { |line| rp_list << { 'pid' => line.strip } } if File.exist?(reprocess)
                rp_list
              else
                log.debug('Processing api response')
                get_cwrc_objs(cookie, start_dt)
              end
  log.debug("Number of objects to process: #{cwrc_objs.nil? ? '0' : cwrc_objs&.count}")

  # for each cwrc object
  cwrc_objs&.each do |cwrc_obj|
    cwrc_file = "#{cwrc_obj['pid'].tr(':', '_')}.zip"
    cwrc_file_tmp_path = File.join(work_dir, cwrc_file)

    log.debug("PROCESSING OBJECT: #{cwrc_obj['pid']}, modified timestamp #{cwrc_obj['timestamp']}")

    # check if file has been deposited, handle open stack bug causing exception in openstack/connection
    force_deposit = false || !reprocess.empty?
    begin
      swift_file = swift_depositer.get_file_from_swit(cwrc_file, ENV['CWRC_SWIFT_CONTAINER']) unless force_deposit
    rescue StandardError => e
      force_deposit = true
      log.debug("Force deposit in swift: #{cwrc_obj['pid']} #{e.message}")
    end

    # if object does not exist within Swift, or is outdated, or has a zero size, or has been marked for a forced update
    next unless force_deposit ||
                swift_file.nil? ||
                swift_file.bytes.to_f.zero? ||
                swift_file.metadata['timestamp'].nil? ||
                cwrc_obj['timestamp'].to_time > swift_file.metadata['timestamp'].to_time

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

    # deposit into swift and remove downloaded file, handle swift errors
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
