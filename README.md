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
 -r --reprocess <file> path to file containing IDs, one per line, for processing 

cwrc_preserver.rb [options] | tee mylogfile.txt
```
   cwrc_preserver.rb will create two output files (in addition to displaying messages to STDOUT if debug enabled), these files set in
   secret.yml file. swift_archived_objs.txt lists the IDs, size and archive rate of all CWRC successfully archived objects,
   Second file, swift_failed_objs.txt, lists all CWRC objects that 
   failed to archive in SWIFT - this will need review and are candidates for reprocessing (hence -r parameter)

 - to view object in CWRC but not in Swift (or outdated within Swift), execute
   - cwrc_reconcile.rb
   - cwrc_audit_report.rb - also find items in Swift but not CWRC and adds details to the report to help audit. 

```shell
cwrc_reconcile.rb
```
   This program will print to STDOUT all CWRC objects that are in CWRC but not in SWIFT or have newer modified date in CWRC.
   It also creates two output files swift_missing_objs.txt - containing all objects that needs to be archived,
   second file swift_objs.txt - listing all CWRC objects that are in SWIFT and have same modified date.

It is recommended that you run it in debug mode for the first time to see what it is doing as it might take long
time to run it. All debug messages redirect to STDOUT by default.

### Reprocessing files
   if -r parameter specified and points to a file containing a list
   of CWRC objects that need to be re-processed, cwrc_preserver will reprocess the specified files (will download them from
   CWRC repo and archive into SWIFT even if that object is already in the SWIFT repository)

```shell
cwrc_preserver.rb -r file_name
```

### Reporting / auditing 

```shell
Usage: cwrc_audit_report [options]
    -s, --summary                    Summary output where status is not 'ok'
    -h, --help                       Displays help
```

Builds a CSV formatted audit report comparing content within the CWRC repository relative to UAL's OpenStack Swift preserved content.

The report pulls input from two disparate sources: CWRC repository and UAL OpenStack Swift preservation service. The report links the content based on object id and outputs the linked information in csv rows that included the fields: the CWRC object PIDs and modification date/times, UAL Swift ID, modification time, and size along with a column indicating the preservation status (i.e., indicating if modification time comparison between Swift and CWRC indicates a need for preservation, or if the size of the Swift object is zero, etc)    

The output format is CSV with the following header columns:
```
     CWRC PID,
     CWRC modification,
     Swift ID,
     Swift modification time,
     Swift size,
     Status

     where:
       status =
          if 'x' then needs preservation
           else if 'd' then not present within CWRC
           else if 'x' then Swift object is of zero size
           else '' then ok
```


### Troubleshooting

