##############################################################################################
# desc: migrate swift content from one instance to a second
# usage:
#  source openrc file from the Swift destination then
#  python3 migrate.py \
#   --swift_src_config_path ${SRC_CONFIG_PATH} \
#   --swift_dest_container ${SWIFT_CONTAINER} \
#   --id_list ${id_LIST} \
#   --tmp_dir ${TMP_DIR} \
#   --container_src ${} \
#   --container_dst ${} \
#   --uploaded_by ${}  \
#   --database_csv ${}
# https://docs.openstack.org/python-swiftclient/latest/service-api.html
# https://docs.openstack.org/swift/pike/overview_large_objects.html#additional-notes
# https://docs.openstack.org/python-swiftclient/latest/service-api.html
# https://github.com/openstack/python-swiftclient/blob/master/swiftclient/client.py#L516
#
# date: Sept 12, 2023
##############################################################################################

import argparse
import csv
import hashlib
import logging
import os
import sys
import yaml
from swiftclient.service import ClientException, SwiftError, SwiftService, SwiftUploadObject

# Swift: length of a segment; created when the file is too large
SWIFT_SEGMENT_LENGTH = 5261334937


#
def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--swift_src_config_path', required=True, help='Source preservation container name.')
    parser.add_argument('--id_list', required=True, help='Migrate only the items in the file (IDs; one per line).')
    parser.add_argument('--tmp_dir', required=True, help='Temporary directory (must exist; used for tar).')
    parser.add_argument('--container_src', required=True, help='Source container name.')
    parser.add_argument('--container_dst', required=True, help='Destination container name.')
    parser.add_argument('--uploaded_by', required=True, help='Name of person running the script.')
    parser.add_argument('--database_csv', required=True, help='Name of the log file to store the uploaded item information.')
    return parser.parse_args(args)


# Source Swift
def swift_init_src(config_file, tmp_dir='/tmp'):
    with open(config_file, "r") as stream:
        try:
            cfg = yaml.safe_load(stream)
            _options = {
                'os_auth_url': cfg['OS_AUTH_URL'],
                'os_username': cfg['OS_USERNAME'],
                'os_password': cfg['OS_PASSWORD'],
                'os_user_domain_name': cfg['OS_USER_DOMAIN_NAME'],
                'os_project_domain_name': cfg['OS_PROJECT_DOMAIN_NAME'],
                'os_project_name': cfg['OS_PROJECT_NAME'],
                'os_project_id': '',  # set blank when using UAL swift plus OLRC source env variables otherwise auth fails
                'os_project_domain_id': '',  # set blank when using UAL swift plus OLRC source env variables otherwise auth fails
                'os_region_name': '',  # set blank when using UAL swift plus OLRC source env variables otherwise auth fails
                'retries': 2,
                'out_directory': tmp_dir
            }
            conn = SwiftService(options=_options)

        except yaml.YAMLError as e:
            print(e)
        except ClientException as e:
            print(e)
        except Exception as e:
            print(e)
    return conn


def container_info(swift_conn_src, container_src, swift_conn_dst, container_dst):
    tmp = swift_conn_src.stat(container_src)
    print(f"{tmp}")
    tmp = swift_conn_dst.stat(container_dst)
    print(f"{tmp}")


# validate that the contents of the destination match the source; allow exceptions in the header
# for example: timestamp and source Swift (CWRC) contains the wrong mimetype (fixed during the upload)
def validate(swift_conn_src, container_src, swift_conn_dst, container_dst, id, exceptions=[]):
    if type(id) is not list:
        id = [id]

    # header properties that are expected to be different, e.g., request related ids or Swift managed timestamps
    exceptions = [
        *exceptions,
        'last-modified',
        'x-timestamp',
        'x-trans-id',
        'x-openstack-request-id',
        'date'
    ]
    # stat returns an iterator: https://docs.openstack.org/python-swiftclient/latest/service-api.html#stat
    for src in swift_conn_src.stat(container_src, id):
        logging.info(f"{src}")
        for dst in swift_conn_dst.stat(container_dst, id):
            logging.info(f"{dst}")
            for key in src['headers']:
                if key not in exceptions:
                    if key not in dst['headers']:
                        logging.error(f"{key} not present in destination: {src['headers'][key]}")
                        raise SwiftError(f"{key} not present in destination: {src['headers'][key]}", container_dst, id)
                    elif container_src == 'CWRC' and key == 'content-type' and dst['headers'][key] == 'application/zip':
                        logging.info(f"{key} differs; this is expected in CWRC due to bulk change - destination {dst['headers'][key]}")
                    elif src['headers'][key] != dst['headers'][key]:
                        logging.error(f"{key} differs {src['headers'][key]} <> {dst['headers'][key]}")
                        raise SwiftError(f"{key} differs: {src['headers'][key]} <> {dst['headers'][key]}", container_dst, id)
                    else:
                        logging.info(f"{key} matches {src['headers'][key]} == {dst['headers'][key]}")


#
def file_checksum(path):
    hash_md5 = hashlib.md5()
    hash_sha256 = hashlib.sha256()
    with open(path, 'rb') as f:
        # read and buffer to prevent high memory usage
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
            hash_sha256.update(chunk)
    return {
        'md5sum': hash_md5.hexdigest(),
        'sha256sum': hash_sha256.hexdigest()
    }


#
def validate_checksum(path, etag, id):
    checksums = file_checksum(path)
    if checksums['md5sum'] != etag:
        raise ClientException(f"ERROR: id:[{id}] error: checksum failure [{path}] - {checksums['md5sum']} <> {etag}")
    return checksums


#
def build_swift_upload_object(src_item, container_src):
    # Custom headers: https://github.com/ualbertalib/swift_ingest/blob/master/lib/swift_ingest/ingestor.rb#L18
    options = {
        'header': {
            'x-object-meta-project-id': src_item['response_dict']['headers']['x-object-meta-project-id'],
            'x-object-meta-aip-version': src_item['response_dict']['headers']['x-object-meta-aip-version'],
            'x-object-meta-project': src_item['response_dict']['headers']['x-object-meta-project'],
            'x-object-meta-promise': src_item['response_dict']['headers']['x-object-meta-promise'],
            'content-type': src_item['response_dict']['headers']['content-type']
        }
    }
    # Custom CWRC metadata used by auditing processes; CWRC platform object last update timestamp
    # CWRC content-type fix (source Swift used x-tar when content is zip)
    if container_src == 'CWRC':
        # could use |= to combine dict structures but aiming for Python 3.5
        options['header']['x-object-meta-last-mod-timestamp'] = src_item['response_dict']['headers']['x-object-meta-last-mod-timestamp']
        options['header']['content-type'] = 'application/zip'

    upload_obj = SwiftUploadObject(
        src_item['path'],
        object_name=src_item['object'],
        options=options
    )

    return upload_obj


#
def download_from_source(swift_conn_src, container_src, id):
    if type(id) is not list:
        id = [id]

    # download the Swift object from the source Swift instance
    src_objs = swift_conn_src.download(container_src, id)

    # build SwiftUploadObject from download response
    dst_objs = []
    for src_item in src_objs:

        logging.info(f"{src_item}")
        if not src_item['success']:
            raise ClientException(f"ERROR: id:[{id}] error: {src_item['error']}")

        dst_objs.append(build_swift_upload_object(src_item, container_src))

        # test download file against Swift header etag to verify
        validate_checksum(src_item['path'], src_item['response_dict']['headers']['etag'], id)

    return dst_objs


#
def log_upload(db_writer, dst_item, container_dst, checksums, uploaded_by):
    db_dict = {
        'id': dst_item['object'],
        'md5sum': checksums['md5sum'],
        'sha256sum': checksums['sha256sum'],
        'uploaded_by': uploaded_by,
        'last_updated_at': dst_item['response_dict']['headers']['last-modified'],
        'container_name': container_dst,
        'notes': ""
    }
    db_writer.writerow(db_dict)


# upload to Swift and remove temporary file
def upload_to_destination(swift_conn_dst, container_dst, dst_objs, db_writer, uploaded_by):
    for dst_item in swift_conn_dst.upload(container_dst, dst_objs):
        if dst_item['action'] == 'upload_object':
            logging.info(f"{dst_item}")
        if not dst_item['success']:
            if 'object' in dst_item:
                logging.error(f"{dst_item}")
                raise SwiftError(dst_item['error'], container_dst, dst_item['object'])
            # Swift segmented object
            elif 'for_object' in dst_item:
                logging.error(f"{dst_item}")
                raise SwiftError(dst_item['error'], container_dst, dst_item['object'], dst_item['segment_index'])

        if dst_item['action'] == 'upload_object' and os.path.isfile(dst_item['path']):
            # test upload file against Swift header etag to verify
            checksums = validate_checksum(dst_item['path'], dst_item['response_dict']['headers']['etag'], dst_item['object'])
            # log upload
            log_upload(db_writer, dst_item, container_dst, checksums, uploaded_by)
            # remove temporary file
            os.remove(dst_item['path'])


#
def process(args, swift_conn_src, swift_conn_dst, db_writer):

    # get list of items
    try:
        with open(args.id_list) as f:
            for line in f:
                id = line.strip()
                print(id)
                dst_objs = download_from_source(swift_conn_src, args.container_src, id)
                upload_to_destination(swift_conn_dst, args.container_dst, dst_objs, db_writer, args.uploaded_by)
                validate(swift_conn_src, args.container_src, swift_conn_dst, args.container_dst, id)

    except ClientException as e:
        logging.error(e)
    # except Exception as e:
        # logging.error(e)
    finally:
        db_writer.flush()
        os.fsync()


#
def csv_init(fd):
    db_writer = csv.DictWriter(fd, fieldnames=[
        'id',                # (avalon noid)
        'md5sum',
        'sha256sum',
        'uploaded_by',
        'last_updated_at',
        'container_name',
        'notes'
    ])
    db_writer.writeheader()
    return db_writer


#
def main():
    options = {}
    logging.basicConfig(level=logging.ERROR)

    if (os.environ.get('OS_AUTH_URL') is None):
        print("ERROR: missing Swift auth; source the Swift env file for the container before running this script")
        exit()

    args = parse_args(sys.argv[1:])

    options['segment_size'] = SWIFT_SEGMENT_LENGTH
    options['use_slo'] = True
    options['object_uu_threads'] = 1
    options['retries'] = 2
    options['out_directory'] = args.tmp_dir

    with SwiftService(options=options) as swift_conn_dest:
        swift_conn_src = swift_init_src(args.swift_src_config_path, args.tmp_dir)
        with open(args.database_csv, 'w', newline='') as db_file:
            db_writer = csv_init(db_file)

            process(args, swift_conn_src, swift_conn_dest, db_writer)


if __name__ == "__main__":
    main()
