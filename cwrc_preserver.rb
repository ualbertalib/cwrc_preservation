#!/usr/bin/env ruby
#   Usage: <progname> [options]...
#   options
#    -h --help  display help
#    -d --debug run in debug mode
#    -s --start <timestamp> retrieve sub-set defined by modified timestamp
require 'swift_ingest'
require 'optparse'
require 'logger'
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

  # setup logger and log level
  log = Logger.new(STDOUT)
  log.level = Logger::DEBUG if debug_level

  log.debug("Retrieving all objects modified since: #{start_dt}")

  # set environment
  set_env

  # get connection cookie
  cookie = retrieve_cookie
  log.debug("Using connecion cookie: #{cookie}")

  # connect to swift storage
  log.debug("SWIFT username #{ENV['SWIFT_USERNAME']}")
  log.debug("SWIFT password #{ENV['SWIFT_PASSWORD']}")
  log.debug("SWIFT  tenant #{ENV['SWIFT_TENANT']}")
  log.debug("SWIFT  auth_url #{ENV['SWIFT_AUTH_URL']}")
  log.debug("SWIFT  project_name #{ENV['SWIFT_PROJECT_NAME']}")
  log.debug("SWIFT  project_domain_name #{ENV['SWIFT_PROJECT_DOMAIN_NAME']}")
  log.debug("SWIFT  project #{ENV['SWIFT_PROJECT']}")

  swift_depositer = SwiftIngest::Ingestor.new(username: ENV['SWIFT_USERNAME'],
                                              password: ENV['SWIFT_PASSWORD'],
                                              tenant: ENV['SWIFT_TENANT'],
                                              auth_url: ENV['SWIFT_AUTH_URL'],
                                              project_name: ENV['SWIFT_PROJECT_NAME'],
                                              project_domain_name: ENV['SWIFT_PROJECT_DOMAIN_NAME'],
                                              project: ENV['SWIFT_PROJECT'])

  raise CWRCArchivingError if swift_depositer.nil?

  # get list of all objects from cwrc
  cwrc_objs = get_cwrc_objs(cookie, start_dt)
  log.debug("Number of objects to precess: #{cwrc_objs.length}")

  # for each cwrc object
  cwrc_objs.each do |cwrc_obj|
    cwrc_file = "#{cwrc_obj['pid'].to_s.tr(':', '_')}.zip"
    log.debug("PROCESSING: #{cwrc_file}, modified timestamp #{cwrc_obj['timestamp']}")

    # check if file has been deposited
    swift_file = swift_depositer.get_file_from_swit(cwrc_file, ENV['CWRC_SWIFT_CONTAINER'])

    # if object is not is swift or we have newer object
    next unless swift_file.nil? || cwrc_obj['timestamp'].to_s.to_time > swift_file.metadata['timestamp'].to_s.to_time

    # download object from cwrc
    log.debug("DOWNLOADING: #{cwrc_file} is not in swift downloding....")
    download_cwrc_obj(cookie, cwrc_obj, cwrc_file)
    raise CWRCArchivingError unless File.exist?(cwrc_file)

    # deposit into swift an remove it
    swift_depositer.deposit_file(cwrc_file, ENV['CWRC_SWIFT_CONTAINER'], timestamp: cwrc_obj['timestamp'])
    FileUtils.rm_rf(cwrc_file) if File.exist?(cwrc_file)
    log.debug("DEPOSITING: #{cwrc_file} deposited in swift successfully")
  end
end
