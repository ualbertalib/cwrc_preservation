require 'swift_ingest/version'
require 'openstack'
require 'mysql2'
require 'active_support'
require 'active_support/core_ext'

# This is a copy of the Ruby Gem swift_ingest-0.4.1/lib/swift_ingest.rb
# Changed to
# * remove the hard coded content type
# * change the deposit_file method such that the basename doesn't remove '.*'
#     when creating an Swift ID - this causes problems with CWRC

class SwiftIngest::Ingestor

  attr_reader :swift_connection, :project

  def initialize(connection = {})
    extra_opt = { auth_method: 'password',
                  service_type: 'object-store' }
    options = connection.merge(extra_opt)
    options[:api_key] = options.delete :password

    @swift_connection = OpenStack::Connection.create(options)
    @project = connection[:project]

    # connect to the database
    @dbcon = if ENV['DB_HOST'] && ENV['DB_USER'] && ENV['DB_PASSWORD'] && ENV['DB_DATABASE']
               Mysql2::Client.new(host: ENV['DB_HOST'],
                                  username: ENV['DB_USER'],
                                  password: ENV['DB_PASSWORD'],
                                  database: ENV['DB_DATABASE'])
             end
  end

  def get_file_from_swit(file_name, swift_container)
    deposited_file = nil
    file_base_name = File.basename(file_name)
    container = swift_connection.container(swift_container)
    deposited_file = container.object(file_base_name) if container.object_exists?(file_base_name)
    deposited_file
  end

  def deposit_file(file_name, content_type, swift_container, custom_metadata = {})
    file_base_name = File.basename(file_name)
    checksum = Digest::MD5.file(file_name).hexdigest
    container = swift_connection.container(swift_container)

    # Add swift metadata with in accordance to AIP spec:
    # https://docs.google.com/document/d/154BqhDPAdGW-I9enrqLpBYbhkF9exX9lV3kMaijuwPg/edit#
    metadata = {
      project: @project,
      project_id: file_base_name,
      promise: 'bronze',
      aip_version: '1.0'
    }.merge(custom_metadata)

    # ruby-openstack wants all keys of the metadata to be named like
    # "X-Object-Meta-{{Key}}" so update them
    metadata.transform_keys! { |key| "X-Object-Meta-#{key}" }

    if container.object_exists?(file_base_name)
      # temporary solution until fixed in upstream:
      # for update: construct hash for key/value pairs as strings,
      # and metadata as additional key/value string pairs in the hash
      headers = { 'etag' => checksum,
                  'content-type' => content_type }.merge(metadata)
      deposited_file = container.object(file_base_name)
      deposited_file.write(File.open(file_name), headers)
    else
      # for creating new: construct hash with symbols as keys, add metadata as a hash within the header hash
      headers = { etag: checksum,
                  content_type: content_type,
                  metadata: metadata }
      # base file name becomes the Swift identifier
      deposited_file = container.create_object(file_base_name, headers, File.open(file_name))
    end

    return deposited_file unless @dbcon

    # update db with deposited file info
    @dbcon.query("INSERT INTO archiveEvent(project, container, ingestTime, \
                  objectIdentifier, objectChecksum, objectSize) \
                  VALUES('#{@project}', '#{swift_container}', now(), '#{file_base_name}', '#{checksum}', \
                  '#{File.size(file_name)}')")
    custom_metadata.each do |key, value|
      @dbcon.query("INSERT INTO customMetadata(eventId, propertyName, propertyValue) \
                    VALUES(LAST_INSERT_ID(), '#{key}', '#{value}' )")
    end

    deposited_file
  end

end
