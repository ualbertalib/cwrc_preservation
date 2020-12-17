#!/usr/bin/env ruby
require 'yaml'
require 'net/http'
require 'json'
require 'http-cookie'

module CWRCPreserver
  class CWRCArchivingError < StandardError; end

  def self.retrieve_cookie
    # Initialize a cookie jar
    jar = HTTP::CookieJar.new

    login_request = Net::HTTP::Post.new(URI.parse("https://#{ENV['CWRC_HOSTNAME']}#{ENV['CWRC_LOGIN_PATH']}"))
    login_request.content_type = 'application/json'
    login_request.body = JSON.dump('username' => ENV['CWRC_USERNAME'],
                                   'password' => ENV['CWRC_PASSWORD'])
    login_response = Net::HTTP.start(ENV['CWRC_HOSTNAME'], ENV['CWRC_PORT'], use_ssl: true) do |http|
      http.request(login_request)
    end

    # Check response code
    raise CWRCArchivingError unless login_response.code == '200'

    login_response.get_fields('Set-Cookie').each do |value|
      jar.parse(value, "https://#{ENV['CWRC_HOSTNAME']}")
    end

    HTTP::Cookie.cookie_value(jar.cookies("https://#{ENV['CWRC_HOSTNAME']}"))
  end

  def self.init_env(env_file = './secrets.yml')
    # read secrets.yml and set up environment vars
    YAML.safe_load(File.open(env_file)).each do |key, value|
      ENV[key] = value
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
    all_obj_response = Net::HTTP.start(ENV['CWRC_HOSTNAME'], ENV['CWRC_PORT'],
                                       use_ssl: true, read_timeout: http_read_timeout) do |http|
      http.request(all_obj_req)
    end

    raise CWRCArchivingError unless all_obj_response.is_a? Net::HTTPSuccess

    all_obj_response.body.slice! timestamp
    JSON.parse(all_obj_response.body)['objects']
  end

  # Given a UUID, connect to the server and download a file
  # retry in event the server or network connection
  # only save a file if successful
  # http://ruby-doc.org/stdlib-2.5.1/libdoc/net/http/rdoc/Net/HTTP.html
  # ToDo: refactor to improve readability
  def self.download_cwrc_obj(cookie, cwrc_obj, cwrc_file)
    # download object from cwrc
    obj_path = "https://#{ENV['CWRC_HOSTNAME']}/islandora/object/#{cwrc_obj['pid']}/manage/bagit_extension"
    obj_req = Net::HTTP::Get.new(URI.parse(obj_path))
    obj_req['Cookie'] = cookie
    retries = [10, 30, 90, 300, 900]
    http_read_timeout = ENV['CWRC_READ_TIMEOUT'].to_i
    begin
      Net::HTTP.start(ENV['CWRC_HOSTNAME'], ENV['CWRC_PORT'],
                      use_ssl: true, read_timeout: http_read_timeout) do |http|

        # response = http.request(obj_req)
        http.request obj_req do |response|
          if response.is_a? Net::HTTPSuccess
            # CWRC response need to have the object's modified timestamp in the header
            raise CWRCArchivingError if response['CWRC-MODIFIED-DATE'].nil?

            cwrc_obj['timestamp'] = response['CWRC-MODIFIED-DATE'].tr('"', '')

            File.open(cwrc_file, 'wb') do |io|
              # save HTTP response to working directory: chunk large file
              response.read_body do |chunk|
                io.write chunk
              end
            end

            # compare md5sum of downloaded with with the HTTP header CWRC-CHECHSUM
            # to detect transport corruption
            raise CWRCArchivingError unless response['CWRC-CHECKSUM'].tr('"', '') == Digest::MD5.file(cwrc_file).to_s
          elsif response.is_a? Net::HTTPServerError
            raise Net::HTTPError.new("Failed request #{obj_path} with http status #{response.code}", response.code)
          else
            raise Net::HTTPError.new("Failed request #{obj_path} with http status #{response.code}", response.code)
          end
        end
      end
    rescue CWRCArchivingError,
           Net::ReadTimeout,
           Net::HTTPBadResponse,
           Net::HTTPHeaderSyntaxError,
           Net::HTTPServerError,
           Net::HTTPError,
           Errno::ECONNRESET,
           Errno::EHOSTUNREACH,
           Errno::EINVAL,
           EOFError
      # retry if exception
      delay = retries.shift
      raise unless delay

      sleep delay
      http_read_timeout += 30
      retry
    end
  end

  def self.connect_to_swift
    SwiftIngest::Ingestor.new(username: ENV['SWIFT_USERNAME'],
                              password: ENV['SWIFT_PASSWORD'],
                              tenant: ENV['SWIFT_TENANT'],
                              auth_url: ENV['SWIFT_AUTH_URL'],
                              project_name: ENV['SWIFT_PROJECT_NAME'],
                              project_domain_name: ENV['SWIFT_PROJECT_DOMAIN_NAME'],
                              project: ENV['CWRC_PROJECT_NAME'])
  end
end
