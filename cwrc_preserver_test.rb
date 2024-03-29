#!/usr/bin/env ruby
require 'rubygems'
require 'test/unit'
require 'vcr'
require_relative 'cwrc_common'

VCR.configure do |config|
  config.cassette_library_dir = 'fixtures/vcr_cassettes'
  config.hook_into :webmock
end

class VCRTest < Test::Unit::TestCase

  TEST_CONFIG = 'secrets_example.yml'.freeze

  def test_init_environment
    assert_nothing_raised do
      CWRCPreserver.init_env(TEST_CONFIG)
    end
    refute_empty ENV['SWIFT_USERNAME']
    refute_empty ENV['SWIFT_PASSWORD']
    refute_empty ENV['SWIFT_TENANT']
    refute_empty ENV['SWIFT_AUTH_URL']
    refute_empty ENV['SWIFT_PROJECT']
    refute_empty ENV['CWRC_HOSTNAME']
    refute_empty ENV['CWRC_LOGIN_PATH']
    refute_empty ENV['CWRC_PORT']
    refute_empty ENV['CWRC_SWIFT_CONTAINER']
    refute_empty ENV['CWRC_USERNAME']
    refute_empty ENV['CWRC_PASSWORD']
  end

  def test_get_cookie
    VCR.use_cassette('cookie') do
      assert_nothing_raised do
        CWRCPreserver.init_env(TEST_CONFIG)
        refute_empty CWRCPreserver.retrieve_cookie
      end
    end
  end

  def test_list_all_objects
    VCR.use_cassette('cookie') do
      VCR.use_cassette('all_objects') do
        assert_nothing_raised do
          CWRCPreserver.init_env(TEST_CONFIG)
          cookie = CWRCPreserver.retrieve_cookie
          cwrc_objs = CWRCPreserver.get_cwrc_objs(cookie, '')
          refute_empty cwrc_objs
          assert cwrc_objs.count == 99_396
        end
      end
    end
  end

  def test_list_updated_objects
    VCR.use_cassette('cookie') do
      VCR.use_cassette('updated_objects') do
        assert_nothing_raised do
          CWRCPreserver.init_env(TEST_CONFIG)
          cookie = CWRCPreserver.retrieve_cookie
          cwrc_objs = CWRCPreserver.get_cwrc_objs(cookie, '2017-01-01T15:29:21.374Z')
          refute_empty cwrc_objs
          assert cwrc_objs.count == 50
        end
      end
    end
  end

  def test_download_object
    VCR.use_cassette('cookie') do
      VCR.use_cassette('download_object') do
        cwrc_obj = { 'pid' => 'islandora:eb608bc8-059b-4cfc-bc13-358823009373' }
        cwrc_file = cwrc_obj['pid']
        assert_nothing_raised do
          CWRCPreserver.init_env(TEST_CONFIG)
          cookie = CWRCPreserver.retrieve_cookie
          CWRCPreserver.download_cwrc_obj(cookie, cwrc_obj, cwrc_file)
        end
        assert !cwrc_obj['timestamp'].nil?
        assert cwrc_obj['timestamp'] = '2015-02-13T18:24.492Z'
        assert File.exist?(cwrc_file)
        FileUtils.rm_rf(cwrc_file) if File.exist?(cwrc_file)
      end
    end
  end

  def test_download_object_http_error
    VCR.use_cassette('cookie') do
      VCR.use_cassette('download_object_http_error') do
        cwrc_obj = { 'pid' => 'islandora:eb608bc8-059b-4cfc-bc13-358823009373' }
        cwrc_file = cwrc_obj['pid']
        assert_raise do
          CWRCPreserver.init_env(TEST_CONFIG)
          cookie = CWRCPreserver.retrieve_cookie
          CWRCPreserver.download_cwrc_obj(cookie, cwrc_obj, cwrc_file)
        end
        assert !File.exist?(cwrc_file)
        FileUtils.rm_rf(cwrc_file) if File.exist?(cwrc_file)
      end
    end
  end

end
