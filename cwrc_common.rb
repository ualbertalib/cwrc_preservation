#!/usr/bin/env ruby
require 'yaml'
require 'net/http'
require 'json'
require 'http-cookie'

module CWRCPerserver
  class CWRCArchivingError < StandardError; end

  def self.retrieve_cookie
    # Initialize a cookie jar
    jar = HTTP::CookieJar.new

    cookie_file = 'connection_cookie.txt'
    if File.exist?(cookie_file)
      jar.load(cookie_file)
      cookie = jar.cookies("https://#{ENV['CWRC_HOSTNAME']}")[0]
      return cookie.cookie_value unless cookie.expired? || cookie.nil?
    end

    login_request = Net::HTTP::Post.new(URI.parse("https://#{ENV['CWRC_HOSTNAME']}#{ENV['CWRC_LOGIN_PATH']}"))
    login_request.content_type = 'application/json'
    login_request.body = JSON.dump('username' => ENV['CWRC_USERNAME'],
                                   'password' => ENV['CWRC_PASSWORD'])
    login_response = Net::HTTP.start(ENV['CWRC_HOSTNAME'], ENV['CWRC_PORT'].to_s.to_i, use_ssl: true) do |http|
      http.request(login_request)
    end

    # Check response code
    raise CWRCArchivingError unless login_response.code.to_s == '200'

    login_response.get_fields('Set-Cookie').each do |value|
      jar.parse(value, "https://#{ENV['CWRC_HOSTNAME']}")
    end

    jar.save(cookie_file)

    HTTP::Cookie.cookie_value(jar.cookies("https://#{ENV['CWRC_HOSTNAME']}"))
  end

  def self.set_env
    # read secrets.yml and set up environment vars
    env_file = './secrets.yml'
    YAML.safe_load(File.open(env_file)).each do |key, value|
      ENV[key.to_s] = value
    end
  end

  def self.get_cwrc_objs(cookie, timestamp)
    audit_str = if timestamp.length.positive?
                  "audit_by_date/#{timestamp}"
                else
                  'audit'
                end
    all_obj_uri = URI.parse("https://#{ENV['CWRC_HOSTNAME']}/services/bagit_extension/#{audit_str}")
    all_obj_req = Net::HTTP::Get.new(all_obj_uri)
    all_obj_req['Cookie'] = cookie
    http_read_timeout = ENV['CWRC_READ_TIMEOUT'].to_i
    all_obj_response = Net::HTTP.start(ENV['CWRC_HOSTNAME'], ENV['CWRC_PORT'].to_s.to_i,
                                       use_ssl: true, read_timeout: http_read_timeout) do |http|
      http.request(all_obj_req)
    end
    all_obj_response.body.slice! timestamp
    JSON.parse(all_obj_response.body)['objects']
  end

  def self.download_cwrc_obj(cookie, cwrc_obj, cwrc_file)
    # download object from cwrc
    obj_path = "https://#{ENV['CWRC_HOSTNAME']}/islandora/object/#{cwrc_obj['pid']}/manage/bagit_extension"
    obj_req = Net::HTTP::Get.new(URI.parse(obj_path))
    obj_req['Cookie'] = cookie
    retries = [10, 30, 90, 300, 900]
    http_read_timeout = ENV['CWRC_READ_TIMEOUT'].to_i
    begin
      obj_response = Net::HTTP.start(ENV['CWRC_HOSTNAME'], ENV['CWRC_PORT'].to_s.to_i,
                                     use_ssl: true, read_timeout: http_read_timeout) do |http|
        http.request(obj_req)
      end
    rescue Net::ReadTimeout
      delay = retries.shift
      raise unless delay
      sleep delay
      http_read_timeout += 30
      retry
    end
    open(cwrc_file, 'wb') do |file|
      file.write(obj_response.body)
    end
  end

  def self.connect_to_swift
    SwiftIngest::Ingestor.new(username: ENV['SWIFT_USERNAME'],
                              password: ENV['SWIFT_PASSWORD'],
                              tenant: ENV['SWIFT_TENANT'],
                              auth_url: ENV['SWIFT_AUTH_URL'],
                              project_name: ENV['SWIFT_PROJECT_NAME'],
                              project_domain_name: ENV['SWIFT_PROJECT_DOMAIN_NAME'],
                              project: ENV['SWIFT_PROJECT'])
  end
end
