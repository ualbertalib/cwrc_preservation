#!/usr/bin/env ruby
require 'yaml'
require 'net/http'
require 'json'
require 'swift_ingest'
require 'http-cookie'
require 'json'
require 'swift_ingest'

module CWRCPerserver

  class CWRCArchivingError < StandardError; end

  def self.get_cookie()

    # Initialize a cookie jar
    jar = HTTP::CookieJar.new

    cookie_file = 'connection_cookie.txt'
    if File.exist?(cookie_file)
      jar.load(cookie_file)
      cookie = jar.cookies("https://#{@cwrc_hostname}")[0]
      return cookie.cookie_value unless cookie.expired? || cookie.nil?
    end

    login_request = Net::HTTP::Post.new(URI.parse("https://#{@cwrc_hostname}#{@cwrc_login_path}"))
    login_request.content_type = "application/json"
    login_request.body = JSON.dump({
                                       "username" => "ualbertalib",
                                       "password" => "m2ey8V2xnJM22kiN"
                                   })
    login_response = Net::HTTP.start(@cwrc_hostname, @cwrc_port, @cwrc_options) do |http|
      http.request(login_request)
    end

    # Check response code
    raise CWRCArchivingError unless login_response.code.to_s == "200"

    login_response.get_fields('Set-Cookie').each do |value|
      jar.parse(value, "https://#{@cwrc_hostname}")
    end

    jar.save(cookie_file)

    HTTP::Cookie.cookie_value(jar.cookies("https://#{@cwrc_hostname}"))
  end

  def self.set_env()
    # read secrets.yml and set up environment vars
    env_file = './secrets.yml'
    raise CWRCArchivingError if !File.exists?(env_file)
    YAML.load(File.open(env_file)).each do |key, value|
      ENV[key.to_s] = value
    end
  end

  def self.get_cwrc_objs(cookie)
    all_obj_uri = URI.parse("https://#{@cwrc_hostname}/services/bagit_extension/audit")
    all_obj_req = Net::HTTP::Get.new(all_obj_uri)
    all_obj_req['Cookie'] = cookie
    all_obj_response = Net::HTTP.start(@cwrc_hostname, @cwrc_port, @cwrc_options) do |http|
      http.request(all_obj_req)
    end
    (JSON.parse(all_obj_response.body))['objects']
  end

  def self.download_cwrc_obj(cookie, cwrc_obj, cwrc_file)
    #download object from cwrc
    obj_path = "https://#{@cwrc_hostname}/islandora/object/#{cwrc_obj['pid']}/manage/bagit_extension"
    obj_req = Net::HTTP::Get.new(URI.parse(obj_path))
    obj_req['Cookie'] = cookie
    obj_response = Net::HTTP.start(@cwrc_hostname, @cwrc_port, @cwrc_options) do |http|
      http.request(obj_req)
    end

    open(cwrc_file, "wb") do |file|
      file.write(obj_response.body)
    end if obj_response.code.to_s == '200'
   end


  # set environment
  set_env

  # cwrc login credentials
  @cwrc_hostname = ENV['CWRC_HOSTNAME']
  @cwrc_login_path = ENV['CWRC_LOGIN_PATH']
  @cwrc_port = ENV['CWRC_PORT'].to_s.to_i
  @cwrc_options = {
      use_ssl: true
  }
  cwrc_swift_container = ENV['CWRC_SWIFT_CONTAINER']

  # get connection cookie
  cookie = get_cookie()


  # connect to swift storage
  swift_depositer = SwiftIngest::Ingestor.new(username: ENV['SWIFT_USERNAME'],
                                              password: ENV['SWIFT_PASSWORD'],
                                              tenant: ENV['SWIFT_TENANT'],
                                              auth_url: ENV['SWIFT_AUTH_URL'],
                                              project_name: ENV['SWIFT_PROJECT_NAME'],
                                              project_domain_name: ENV['SWIFT_PROJECT_DOMAIN_NAME'],
                                              project: ENV['SWIFT_PROJECT'])

  raise CWRCArchivingError if swift_depositer.nil?

  # get list of all objects from cwrc
  cwrc_objs=  get_cwrc_objs(cookie)

  # for each cwrc object
  cwrc_objs.each do |cwrc_obj|

    cwrc_file = "#{cwrc_obj['pid'].to_s.tr(':', '_')}.zip"

    # check if file has been deposited
    swift_file = swift_depositer.get_file_from_swit(cwrc_file, cwrc_swift_container )

    # if object is not is swift or we have newer object
    if swift_file.nil? || cwrc_obj['timestamp'].to_s.to_time > swift_file.metadata['timestamp'].to_s.to_time

      #download object from cwrc
      download_cwrc_obj(cookie, cwrc_obj, cwrc_file)
      raise CWRCArchivingError if !File.exist?(cwrc_file)

      # deposit into swift an remove it
      swift_depositer.deposit_file(cwrc_file, cwrc_swift_container, {timestamp: cwrc_obj['timestamp'].to_s})
      FileUtils.rm_rf(cwrc_file) if File.exist?(cwrc_file)
    end

  end

end
