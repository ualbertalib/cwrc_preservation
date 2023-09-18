""" Very quickly write unit tests for a one-time script
"""


import csv
import os
import pytest
import pytest_mock
import shutil
import sys

from swiftclient.service import ClientException, SwiftError, SwiftService, SwiftUploadObject

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import migrate as migrate

_object_id = 'a:1'
_swift_download_response = {
    'success': True,
    'path': 'tests/fixtures/assets/a:1',
    'object': _object_id,
    'response_dict': {
        'headers': {
            'etag': '94813657ffbc76defd96ac21ff4061ca',
            'x-object-meta-project-id': 'a',
            'x-object-meta-aip-version': 'b',
            'x-object-meta-project': 'c',
            'x-object-meta-promise': 'd',
            'content-type': 'e',
            'x-object-meta-last-mod-timestamp': 'f'
        }
    }
}


# CWRC object - success on mismatched content-type
def test_validate(mocker):
    mocker.patch('migrate.SwiftService.stat', side_effect=[
        [{'headers': {'a': 'b', 'date': '2000', 'content-type': 'application/x-tar'}}],
        [{'headers': {'a': 'b', 'date': '2001', 'content-type': 'application/zip'}}]
    ])
    try:
        migrate.validate(SwiftService, 'CWRC', SwiftService, 'cwrc', 'a:1')
        assert True
    except Exception:
        assert False


def test_validate_missing_header(mocker):
    mocker.patch('migrate.SwiftService.stat', side_effect=[
        [{'headers': {'a': 'b', 'date': '2000', 'content-type': 'application/x-tar'}}],
        [{'headers': {'date': '2001', 'content-type': 'application/zip'}}]
    ])
    with pytest.raises(SwiftError) as excinfo:
        migrate.validate(SwiftService, 'CWRC', SwiftService, 'cwrc', 'a:1')
    assert str(excinfo.value) == "'a not present in destination: b' container:cwrc object:['a:1']"


def test_validate_header_value_difference(mocker):
    mocker.patch('migrate.SwiftService.stat', side_effect=[
        [{'headers': {'a': 'b', 'date': '2000', 'content-type': 'application/x-tar'}}],
        [{'headers': {'a': 'invalid', 'date': '2001', 'content-type': 'application/zip'}}]
    ])
    with pytest.raises(SwiftError) as excinfo:
        migrate.validate(SwiftService, 'CWRC', SwiftService, 'cwrc', 'a:1')
    assert str(excinfo.value) == "'a differs: b <> invalid' container:cwrc object:['a:1']"


# non CWRC object - error on mismatched content-type
def test_validate_header_content_type(mocker):
    mocker.patch('migrate.SwiftService.stat', side_effect=[
        [{'headers': {'date': '2000', 'content-type': 'application/x-tar'}}],
        [{'headers': {'date': '2001', 'content-type': 'application/zip'}}]
    ])
    with pytest.raises(SwiftError) as excinfo:
        migrate.validate(SwiftService, 'x', SwiftService, 'x', 'a:1')
    assert str(excinfo.value) == "'content-type differs: application/x-tar <> application/zip' container:x object:['a:1']"


def test_download_from_source_cwrc(mocker):
    mocker.patch('migrate.SwiftService.download', return_value=[
        {
            'success': True,
            'path': 'tests/fixtures/assets/a:1',
            'object': _object_id,
            'response_dict': {
                'headers': {
                    'etag': '94813657ffbc76defd96ac21ff4061ca',
                    'x-object-meta-project-id': 'a',
                    'x-object-meta-aip-version': 'b',
                    'x-object-meta-project': 'c',
                    'x-object-meta-promise': 'd',
                    'content-type': 'e',
                    'x-object-meta-last-mod-timestamp': 'f'
                }
            }
        }
    ])
    upload_obj = migrate.download_from_source(SwiftService, 'CWRC', object)
    for item in upload_obj:
        assert item.object_name == _object_id
        assert item.options['header']['x-object-meta-project-id'] == 'a'
        assert item.options['header']['x-object-meta-aip-version'] == 'b'
        assert item.options['header']['x-object-meta-project'] == 'c'
        assert item.options['header']['x-object-meta-promise'] == 'd'
        assert item.options['header']['content-type'] == 'application/zip'
        assert item.options['header']['x-object-meta-last-mod-timestamp'] == 'f'
    # non-cwrc test
    upload_obj = migrate.download_from_source(SwiftService, '', object)
    for item in upload_obj:
        assert item.options['header']['content-type'] == 'e'
        assert 'x-object-meta-last-mod-timestamp' not in item.options['header']


def test_upload_to_destination(tmpdir, mocker):
    t = _swift_download_response
    t['path'] = tmpdir / _object_id
    upload_obj = [migrate.build_swift_upload_object(t, 'cwrc')]
    shutil.copy('tests/fixtures/assets/a:1', t['path'])
    csv_path = tmpdir / "csv"
    with open(csv_path, 'w', newline='') as csv_fd:
        csv_dict = migrate.csv_init(csv_fd)
        mocker.patch('migrate.SwiftService.upload', return_value=[
            {
                'action': 'upload_object',
                'success': True,
                'path': t['path'],
                'object': _object_id,
                'response_dict': {
                    'headers': {
                        'etag': '94813657ffbc76defd96ac21ff4061ca',
                        'last-modified': 'a'
                    }
                }
            }
        ])
        migrate.upload_to_destination(SwiftService, 'CWRC', upload_obj, csv_dict, "J")
        assert not os.path.exists(t['path'])
    with open(csv_path, 'r', newline='') as tmp_fd:
        dr = csv.DictReader(tmp_fd)
        for row in dr:
            assert row['id'] == _object_id
