#!/usr/bin/env ruby
require 'yaml'
require 'net/http'
require 'json'
require 'swift_ingest'
require 'http-cookie'
require 'json'
require 'swift_ingest'

module CWRCPerserver

  class ErrorConnetingToCWRC < StandardError; end

  # read secrets.yml and set up environment vars
  env_file = './secrets.yml'
  YAML.load(File.open(env_file)).each do |key, value|
    ENV[key.to_s] = value
  end if File.exists?(env_file)

  # create swift object
  swift_depositer = SwiftIngest::Ingestor.new(username: 'test:tester',
                                              password: 'testing',
                                              tenant: 'tester',
                                              auth_url: 'http://127.0.0.1:8080/auth/v1.0',
                                              project: 'ERA')

  #swift_depositer = SwiftIngest::Ingestor.new(username: ENV['SWIFT_USERNAME'],
  #                                            password: ENV['SWIFT_PASSWORD'],
  #                                            tenant: ENV['SWIFT_TENANT'],
  #                                            auth_url: ENV['SWIFT_AUTH_URL'],
  #                                            project_name: ENV['SWIFT_PROJECT_NAME'],
  #                                            project_domain_name: ENV['SWIFT_PROJECT_DOMAIN_NAME'],
  #                                            project: ENV['SWIFT_PROJECT'])

  # login into cwrc and get cookie
  cwrc_server = 'https://cwrc-dev-05.srv.ualberta.ca'
  cwrc_login_path='/rest/user/login'
  login_uri = URI.parse("#{cwrc_server}#{cwrc_login_path}")
  login_request = Net::HTTP::Post.new(login_uri)
  login_request.content_type = "application/json"
  login_request.body = JSON.dump({
    "username" => "ualbertalib",
    "password" => "m2ey8V2xnJM22kiN"
  })

  req_options = {
    use_ssl: login_uri.scheme == "https",
  }

  login_response = Net::HTTP.start(login_uri.hostname, login_uri.port, req_options) do |http|
        http.request(login_request)
  end

  # Check response code
  raise ErrorConnetingToCWRC unless login_response.code.to_s == "200"

  # Initialize a cookie jar
  jar = HTTP::CookieJar.new

  login_response.get_fields('Set-Cookie').each do |value|
    jar.parse(value, cwrc_server)
  end

  # Save to a file
  jar.save('connection_cookie.txt')


  # Load from a file
  # jar.load(filename) if File.exist?(filename)

  # fetch json file and break it into individual object
  cwrc_all_objects_path='/services/bagit_extension/audit'
  all_obj_uri = URI.parse("#{cwrc_server}#{cwrc_all_objects_path}")
  all_obj_req = Net::HTTP::Get.new(all_obj_uri)
  all_obj_req['Cookie'] = HTTP::Cookie.cookie_value(jar.cookies(cwrc_server))
  all_obj_req_options = {
      use_ssl: all_obj_uri.scheme == "https",
  }

  all_obj_response = Net::HTTP.start(all_obj_uri.hostname, all_obj_uri.port, all_obj_req_options) do |http|
    http.request(all_obj_req)
  end

  cwrc_objs= (JSON.parse(all_obj_response.body))['objects']


  # for each object
  cwrc_objs.each do |cwrc_obj|

    #download object from cwrc
    obj_uri = URI.parse("https://cwrc-dev-05.srv.ualberta.ca/islandora/object/#{cwrc_obj['pid']}/manage/bagit_extension")
    obj_req = Net::HTTP::Get.new(obj_uri)

    obj_req['Cookie'] = HTTP::Cookie.cookie_value(jar.cookies(cwrc_server))
    obj_req_options = {
        use_ssl: obj_uri.scheme == "https",
    }
    obj_response = Net::HTTP.start(obj_uri.hostname, obj_uri.port, obj_req_options) do |http|
      http.request(obj_req)
    end

    cwrc_file = "#{cwrc_obj['pid'].to_s.tr(':', '_')}.zip"
    open(cwrc_file, "wb") do |file|
      file.write(obj_response.body)
    end if obj_response.code.to_s == '200'


    #deposit into swift
    swift_depositer.deposit_file(cwrc_file, 'CWRC')

  end




  #    response = Net::HTTP.get_response(uri)
  # check if it is already in swift
  # if no
  #    download object from cwrc
  #    uri = URI.parse("https://cwrc-dev-05.srv.ualberta.ca/islandora/object/orlando:1bdcd4f4-11f5-4bb0-b6a4-5caaaca2079a/manage/bagit_extension")
  #    response = Net::HTTP.get_response(uri)
  #
  #    check checksum of the download object against json header
  #    verify bag with bagit gem
  #    ingest object into swift

end
