#!/usr/bin/env ruby
#   Usage: <progname>
#         this program will verify cwrc object agains swift deposits
#         and will report missing OBJs to STDOUT
require 'swift_ingest'
require 'optparse'
require_relative 'cwrc_common'

module CWRCPerserver
  # process command line arguments -h or --help
  file = __FILE__
  ARGV.options do |opts|
    opts.on_tail('-h', '--help') { exec "grep ^#[[:space:]]<'#{file}'|cut -c4-" }
    opts.parse!
  end

  # set environment
  set_env

  # get connection cookie
  cookie = retrieve_cookie

  # connect to swift storage
  swift_depositer = connect_to_swift
  raise CWRCArchivingError if swift_depositer.nil?

  # get list of all objects from cwrc
  cwrc_objs = get_cwrc_objs(cookie, '')
  print "Number of objects to verity: #{cwrc_objs.length}\n"

  # for each cwrc object
  cwrc_objs.each do |cwrc_obj|
    cwrc_file_str = cwrc_obj['pid'].to_s
    cwrc_file = cwrc_file_str

    swift_file = swift_depositer.get_file_from_swit(cwrc_file, ENV['CWRC_SWIFT_CONTAINER'])

    # if object is not is swift or we have newer object in cwrc report it
    if swift_file.nil? || cwrc_obj['timestamp'].to_s.to_time > swift_file.metadata['timestamp'].to_s.to_time
      print "OBJECT MISSING FROM SWIFT: #{cwrc_file_str}\n"
      File.open('swift_missing_objs.txt', 'a') { |miss_file| miss_file.write("#{cwrc_file_str}\n") }
    else
      mod_dt = swift_file.metadata['timestamp'].to_s
      File.open('swift_objs.txt', 'a') { |ok_file| ok_file.write("#{cwrc_file_str} #{mod_dt}\n") }
    end
  end
end
