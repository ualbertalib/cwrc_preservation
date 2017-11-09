# CWRC Preservation Script
This script is for retrieve and ingest CWRC objects for preservation

## Initial phase design
It will:
- fetch the JSON Manifes which contains PID and lastModified Fedora 3 timestamp from CWRC for objects that needs to be preserved
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
 -s <timestamp> to retieve objects that have been modified since <timestamp>
```

It is recommended that you run it in debug mode for the first time to see what it is doing as it might take long
time to run it. All debug messages redirected to STDOUT. If you want it to appear in the log file:


```shell
cwrc_preserver.rb [options] | tee mylogfile.txt
```

