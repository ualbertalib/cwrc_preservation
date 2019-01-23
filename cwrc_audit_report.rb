#!/usr/bin/env ruby

# Builds a CSV formatted audit report comparing content within the
# CWRC repository relative to UAL's OpenStack Swift preserved content.
#
# The report pulls input from two disparate sources: CWRC repository and
# UAL OpenStack Swift preservation service. The report links the content based
# on object id and outputs the linked information in csv rows that included the
# fields: the CWRC object PIDs and modification date/times, UAL Swift ID,
# modification time, and size along with a column indicating the preservation
# status (i.e., indicating if modification time comparison between Swift and
# CWRC indicates a need for preservation, or if the size of the Swift object
# is zero, etc)

#
# The output format is CSV with the following header columns:
#     CWRC PID,
#     CWRC modification,
#     Swift ID,
#     Swift modification time,
#     Swift size,
#     Status
#
#     where:
#       status =
#          if 'x' then needs preservation
#           else if 'd' then not present within CWRC
#           else if 'x' then Swift object is of zero size
#           else '' then ok
#
# Usage: <progname> [options]...
#   options
#     -c --config PATH
#     -h --help
#     -s --summary summary output where status in not 'ok'
#

require 'logger'
require 'optparse'
require 'time'
require 'swift_ingest'

require_relative 'cwrc_common'

module CWRCPerserver
  # status IDs
  STATUS_OK = ''.freeze
  STATUS_E_SIZE = 's'.freeze # error: size zero or too small
  STATUS_I_FLAG = 'x'.freeze # flagged for preservation
  STATUS_I_DEL = 'd'.freeze # missing from the CWRC side while Swift contains a copy

  config_file = './secrets.yml'

  opt_summary_output = false
  ARGV.options do |opts|
    opts.on '-C', '--config PATH', 'Path for YAML config file' do |val|
      config_file = val
    end
    opts.on('-s', '--summary', "Summary output where status is not 'ok'") do
      opt_summary_output = true
    end
    opts.on_tail('-h', '--help', 'Displays help') do
      puts opts
      exit
    end
    opts.parse!
  end

  # initialize environment
  init_env(config_file)

  # authenticate to CWRC repository
  cookie = retrieve_cookie

  # query the CWRC repository
  # response: {"pid"=>"cwrc:c1583789-0dad-41d3-8a42-94d7a8e6d451", "timestamp"=>"2018-05-02T17:07:29.028Z"}
  cwrc_objs = get_cwrc_objs(cookie, '')

  # connect to swift storage
  swift_con = connect_to_swift
  raise CWRCArchivingError if swift_con.nil?

  # query Swift storage for a list of objects
  # https://github.com/ruby-openstack/ruby-openstack/wiki/Object-Storage
  # https://github.com/ruby-openstack/ruby-openstack/wiki/Object-Storage
  # response: "cwrc_0c168793-b1ff-453f-a1f6-e1d75f7350be"=>{
  #    :bytes=>"5939",
  #    :content_type=>"application/x-tar",
  #    :last_modified=>"2018-02-05T06:45:23.422720",
  #    :hash=>"dd2b11f239f7f25fb504519b612cf896"
  #  },
  swift_container = swift_con.swift_connection.container(swift_con.project)

  # Iterate via markers
  # https://github.com/ruby-openstack/ruby-openstack/blob/d9c8aa19488062e483771a9168d24f2626fe688b/lib/openstack/swift/container.rb#L100
  swift_objs = swift_container.objects_detail
  while swift_objs.count < swift_container.container_metadata[:count].to_i
    swift_objs = swift_objs.merge(swift_container.objects_detail(marker: swift_objs.keys.last))
  end

  # TODO: use CSV gem
  # CSV header
  puts "cwrc_pid (#{cwrc_objs.count}),"\
    "cwrc_mtime (#{Time.now.iso8601}),"\
    "swift_id (#{swift_container.container_metadata[:count]}),"\
    'swift_timestamp,swift_bytes,status'

  # TODO: find a better way to merge CWRC and Swift hashes into an output format
  # for each cwrc object
  cwrc_objs&.each do |cwrc_obj|
    cwrc_pid = cwrc_obj['pid']
    cwrc_mtime = cwrc_obj['timestamp']
    swift_id = cwrc_pid

    if swift_objs.key?(swift_id)
      swift_timestamp = swift_objs[swift_id][:last_modified]
      swift_bytes = swift_objs[swift_id][:bytes]
      # note: CWRC uses zulu while Swift is local timezone (assumption)
      # If timestamps don't match then report Swift object older than CWRC
      status = if Time.parse(cwrc_mtime) > Time.parse(swift_timestamp)
                 STATUS_I_FLAG
               elsif swift_bytes.to_i < 20 # object too small
                 STATUS_E_SIZE
               else
                 STATUS_OK
               end
      swift_objs.delete(swift_id)
    else
      # Swift missing the CWRC object, status and empty Swift columns reported
      swift_id = ''
      swift_timestamp = ''
      swift_bytes = ''
      status = STATUS_I_FLAG
    end

    # CSV content
    if !opt_summary_output || (opt_summary_output && status != STATUS_OK)
      puts "#{cwrc_pid},#{cwrc_mtime},#{swift_id},#{swift_timestamp},#{swift_bytes},#{status}"
    end
  end

  # find the remaining Swift objects that don't have corresponding items in CWRC
  swift_objs&.each do |key, swift_obj|
    # CSV content
    puts ",,#{key},#{swift_obj[:last_modified]},#{swift_obj[:bytes]},#{STATUS_I_DEL}"
  end
end
