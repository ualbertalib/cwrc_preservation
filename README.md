# CWRC Preservation Script
This script is for retrieve and ingest CWRC objects for preservation

## Initial phase design
It will:
- fetch the JSON Manifest which contains PID and lastModified Fedora 3 timestamp from CWRC for objects that needs to be preserved
- Process each object by the PID in the JSON manifest
- Get the HEADER from the AIP URL, which contains the LastModified timestamp in the ETAG.
- Compare lastModifed timestamp in JSON and in the header ETAG
  - if they are same, check if object is already in Swift, with PID and this timestamp
  - if they are different, use the ETAG timestamp to check if object is already in Swift
- If object is not in Swift, download the zipped AIP, verify the bag, and ingest into Swift.

## Next steps:
  - Add mysql logging to swift_ingest
  - Check tracking in db and verify with info from Swift when checking for duplicates in Swift.

## How to use:
Initial phase has been implemented, script can pull objects from CWRC repository and deposit it into SWIFT.

  - create secret.yml file (there is secret_example.yml) and set an appropriate values in that file

  - run cwrc_preserver.rb script with an appropriate parameters

```shell
cwrc_preserver.rb [options]
 -d --debug to run in debug mode
 -s <timestamp> --start=<timestamp>  to retieve objects that have been modified since <timestamp>
 -h --help display usage
 -r --reprocess re-process cwrc objects specified in re-proces file (specified secret.yml)

cwrc_preserver.rb [options] | tee mylogfile.txt
```
   cwrc_preserver.rb will create two output files (in addition to displaying messages to STDOUT), these files set in
   secret.yml file. First file swift_archived_objs.txt that lists all CWRC successfully archived object,
   object size and archiving rate. Second file swift_failed_objs.txt - lists all CWRC objects that are
   failed to archive in SWIFT, usually they need to be re-processed again (hence -r parameter)

 - to reconcile archived objects between swift and cwrc run cwrc_reconcile.rb.

```shell
cwrc_reconcile.rb
```
   This program will print to STDOUT all CWRC objects that are in CWRC but not in SWIFT or have newer modified date in CWRC.
   It also creates two output files swift_missing_objs.txt - containing all objects that needs to be archived,
   second file swift_objs.txt - listing all CWRC objects that are in SWIFT and have same modified date.

It is recommended that you run it in debug mode for the first time to see what it is doing as it might take long
time to run it. All debug messages redirected to STDOUT. If you want it to appear in the log file:

- Troubleshooting

We have implemented reuse of cookies using connection_cookie.txt file. If CWRC server is reset and will not recognize
previously issued cookie (even though it did not expire), simply delete connection_cookie.txt
