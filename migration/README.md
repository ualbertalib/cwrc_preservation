# Migration from UAL Swift to OLRC Oct. 2023

## Process

1. Use the audit report as a source of IDs in the UAL Swift

``` bash
cut -d ',' -f 3  2023-09-15_cwrc_audit_report.cleaned.csv | sort > all_pids_2023-09-15.csv pids_2023-09-15_part_
```

2023-10-13: the following 3 objects occur twice in the SPARQL query results building the list of PIDs (persistent IDs)  within CWRC and their associated ‘last modified date’ – this is likely a broken delete operation on the triplestore that didn’t completely remove existing triples before adding a new triple. This doesn’t not  impact date on the preserved item but does impact the audit report in that there are two rows form the same PID:

* cwrc:1f59e5c9-63bc-44e2-8146-e612d9aa9a7a
* cwrc:765b9c98-50e1-41a1-83a8-c29e64ce1412
* islandora:db03c412-9e0a-4b79-949b-9bd61bb75bfd

1. Segment the list

Sepment the list of ~410K items into 25k items to ease recovery if the migration process stops (e.g., server reboot, etc.)

``` bash
split -l 25000 -d  all_pids_2023-09-15.csv pids_2023-09-15_part_
```

1. Migrate

``` bash
source "RC_file_from_OLRC_Horizon_UI"
SEGMENT=11
python3 migration/migrate.py --swift_src_config_path ../secret_ual.yml --id_list ../pid_lists/pids_2023-09-15_part_${SEGMENT} --tmp_dir ../tmp/ --container_src CWRC --container_dst cwrc --uploaded_by "Jeffery Antoniuk" --database_csv ../logs/pids_2023-09-15_part_${SEGMENT}.log
```

1. Audit

* test counts
* run the `cwrc_audit_report.rb`
  * audit migrated Swift content comparing one Swift instance to a second
  * see the script for details
* run the 4-week preservation side-by-side old and new and compare the output

## Tests

### How to run tests

Setup

```bash
pip3 install pytest pytest-mock --user
```

Run

```bash
pytest tests/migration_unit_test.py
```