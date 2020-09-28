#!/usr/bin/env python
"""dedupe

Usage: dedupe.py [-p=<project> | --project=<project>] [--start_date=<sd>] [--end_date=<ed>] [--dataset=<ds>]
[--table_suffix=<ts>] [-w | --wait] [-q | --quiet] dedupe.py (-h | --help) dedupe.py --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  -p --project=<project>  gcp project [default: ltv-modeling-user]
  --start_date=<sd>  inclusive start date of table suffix [default: 20200101].
  --end_date=<ed>  inclusive end date of table suffix [default: 20200102].
  --dataset=<ds>  big query dataset name [default: ltv].
  --table_suffix=<ts>  big query table name suffix [default: ltv_].
  -w --wait  wait until all jobs are done [default: False].
  -q --quiet  print debug lines [default: True]

"""

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from time import sleep
import pandas as pd
from docopt import docopt


def remove_duplicates(args,
                      bq_client,
                      project_name,
                      dataset_name,
                      table_name):
    # if not args['--quiet']:
    #     print(project_name)
    #     print(dataset_name)
    #     print(table_name)
    does_table_exist(args,
                     bq_client,
                     project_id=project_name,
                     dataset_id=dataset_name,
                     table_id=table_name)
    query = f"""
        MERGE `{project_name}.{dataset_name}.{table_name}` AS target_t
        USING (
          SELECT a.*
          from (
            select ANY_VALUE(a) a 
            FROM `{project_name}.{dataset_name}.{table_name}` a
            group by TO_JSON_STRING(a)
            )
          ) AS source_t
        ON FALSE
        WHEN NOT MATCHED BY SOURCE THEN DELETE
        WHEN NOT MATCHED BY TARGET THEN INSERT ROW;"""
    # if not args['--quiet']:
    #     print(query)
    job_config = bigquery.QueryJobConfig(priority=bigquery.QueryPriority.BATCH)
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    return query_job


def get_state(bq_client, query_job):
    query_job = bq_client.get_job(
        query_job.job_id, location=query_job.location
    )  # Make an API request.
    return query_job.state


def wait_till_done(args,
                   bq_client,
                   bq_jobs):
    done_pile = [get_state(bq_client, job) == "DONE" for job in bq_jobs]
    while not all(done_pile):
        done_pile = [get_state(bq_client, job) == "DONE" for job in bq_jobs]
        pending_pile = [get_state(bq_client, job) == "PENDING" for job in bq_jobs]
        running_pile = [get_state(bq_client, job) == "RUNNING" for job in bq_jobs]
        if not args['--quiet']:
            print(f"""|{len(bq_jobs)},{sum(pending_pile)},{sum(running_pile)},{sum(done_pile)}|#jobs,#pending,
#running,#done|sleeping for 60s""")
        sleep(60)


def does_table_exist(args, bq, project_id, dataset_id, table_id):
    table_id = f"{project_id}.{dataset_id}.{table_id}"
    try:
        bq.get_table(table_id)  # Make an API request.
    except NotFound:
        print(f"Table {table_id} not found.")
        raise


def remove_duplicates_for_range(args,
                                bq_client,
                                project_name,
                                dataset_name,
                                table_suffix,
                                start_date,
                                end_date):
    dates = [d.strftime('%Y%m%d') for d in pd.date_range(start_date, end_date)]
    if not args['--quiet']:
        print(f"Number of date partitions: {len(dates)}")
    bq_jobs = []
    for date in dates:
        query_job = remove_duplicates(args=args,
                                      bq_client=bq_client,
                                      project_name=project_name,
                                      dataset_name=dataset_name,
                                      table_name=f"{table_suffix}{date}")
        bq_jobs.append(query_job)
    return bq_jobs


def main(args):
    client = bigquery.Client()
    project = args['--project']
    start_date = args['--start_date']
    end_date = args['--end_date']
    dataset = args['--dataset']
    table_suffix = args['--table_suffix']
    jobs = remove_duplicates_for_range(args=args,
                                       bq_client=client,
                                       project_name=project,
                                       dataset_name=dataset,
                                       table_suffix=table_suffix,
                                       start_date=start_date,
                                       end_date=end_date
                                       )

    if args['--wait']:
        wait_till_done(args, client, jobs)


if __name__ == '__main__':
    arguments = docopt(__doc__, version='dedupe.py 0.1')
    main(arguments)
