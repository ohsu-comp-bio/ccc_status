"""
TODO: this only handles workflows with a single call currently.
      this affects the cromwell/condor job status codes.
"""

import argparse
from collections import Counter
from datetime import datetime
import json
import os
from pprint import pprint
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

import ccc_client

argparser = argparse.ArgumentParser()
argparser.add_argument("--status-count", action="store_true")
argparser.add_argument("--only-finished", action="store_true")
argparser.add_argument("--query", nargs="+")
argparser.add_argument("--jobs", nargs="+")

exec_engine_api = ccc_client.ExecEngineRunner()
EXEC_DIR = "/cluster_share/cromwell-executions"

CONDOR_STATUS_CODES = {
    "0": "Unexpanded",
    "1": "Idle",
    "2": "Running",
    "3": "Removed",
    "4": "Completed",
    "5": "Held",
    "6": "Submission_err",
}

def job_path(job_id, *args):
    meta = exec_engine_api.get_metadata(job_id).json()
    # TODO currently this only handles a single "call"
    calls = meta['calls'].keys()
    call_name = 'call-' + calls[0].split('.')[-1]
    workflow_name = meta['workflowName']
    return os.path.join(EXEC_DIR, workflow_name, job_id, call_name, *args)

def get_condor_info():
    output = subprocess.check_output("condor_q -long", shell=True).strip()
    section = {}
    sections = [section]
    for line in output.split("\n"):
	try:
            i = line.index("=")
            key = line[:i - 1]
            value = line[i + 2:]
            if key == "JobStatus":
                value = CONDOR_STATUS_CODES[value]
            section[key] = value
        except ValueError:
            section = {}
            sections.append(section)

    return sections

def get_condor_info_by_job():
    job_info = {}
    for section in get_condor_info():
        # TODO this is expecting a very specific command path
        job_id = section['Cmd'].split("/")[4]
        job_info[job_id] = section
    return job_info

def load_job_configs(paths):
    job_configs = {}
    for path in paths:
        with open(path) as fh:
            job_configs.update(json.load(fh)["multipleInputs"])

    return job_configs

def job_output_file(job_id, file_name):
    p = job_path(job_id, file_name)
    with open(p) as fh:
        return fh.read()

def job_rc(job_id):
    rc_path = job_path(job_id, "rc")
    has_rc = os.path.exists(rc_path)

    if has_rc:
        with open(rc_path) as fh:
            return int(fh.read().strip())

def job_rc_time(job_id):
    rc_path = job_path(job_id, "rc")
    has_rc = os.path.exists(rc_path)

    if has_rc:
        mtime = os.path.getmtime(rc_path)
        return time.strftime("%a, %d %b %Y %H:%M:%S", time.gmtime(mtime))

def did_job_fail(job_id):
    rc = job_rc(job_id)
    return rc is not None and rc != 0

def tail(string, n=20):
    lines = string.split("\n")
    return "\n".join(lines[-n:])

def job_cromwell_log(job_id):
    meta = exec_engine_api.get_metadata(job_id).json()
    workflow_name = meta['workflowName']
    return job_output_file(job_id, workflow_name + '.log')

def parse_event_log(raw):
    events = []

    root = ET.fromstring(raw)
    for c_tag in root:
        event = {}
        events.append(event)

        for a_tag in c_tag:
            name = a_tag.get("n")
            # Get the one and only child element
            #
            # Seems like the ccc/cromwell XML schema only ever has one child
            # for <a> tags, and this line is hard-coded against that expectation.
            child = list(a_tag)[0]
            if child.tag == "i":
                value = int(child.text)
            elif child.tag == "r":
                value = float(child.text)
            elif child.tag == "b":
                if child.get("v") == "t":
                    value = True
                else:
                    value = False
            elif child.tag == "s":
                value = child.text
            else:
                msg = "Unknown attribute type encountered while parsing event log"
                raise Exception(msg)

            event[name] = value

    return events

def cromwell_events(job_id):
    log_content = job_cromwell_log(job_id)
    # The ccc/cromwell output doesn't have a proper root element,
    # so give it one, which makes parsing easier.
    log_content = '<root>' + log_content + '</root>'
    events = parse_event_log(log_content)
    return events

def time_taken(job_id, events):
    start_event = None
    end_event = None

    for event in events:
        if event['MyType'] == 'SubmitEvent':
            start_event = event
        elif event['MyType'] == 'JobTerminatedEvent':
            end_event = event

    if not end_event:
        return 'Still running'

    dt_format = '%Y-%m-%dT%H:%M:%S'
    start_time = datetime.strptime(start_event['EventTime'], dt_format)
    end_time = datetime.strptime(end_event['EventTime'], dt_format)
    return end_time - start_time


################################################################
# Ad hoc query functions

def print_condor_info_keys():
    "Prints all the key names of condor job metadata"
    print "\n".join(get_condor_info()[0].keys())

def print_failed_job_stderr(job_id):
    rc = job_rc(job_id)
    if rc is not None and rc != 0:
        print job_output_file(job_id, "stderr")
        print tail(job_output_file(job_id, "stdout"), n=10)
        print '=' * 50
        print

def print_failed_job_config_path(job_configs):
    for job_id, config_path in job_configs.items():
        if did_job_fail(job_id):
            #print job_id, config_path
            print config_path

def print_job_status_summary(job_ids, condor_jobs_info):
    counts = Counter()
    for job_id in job_ids:
        job_info = condor_jobs_info[job_id]
        job_status = job_info['JobStatus']
        counts[job_status] += 1

    print counts

def print_tail_running_jobs(job_id, condor_jobs_info):
    job_info = condor_jobs_info[job_id]
    job_status = job_info['JobStatus']

    if job_status == "Running":
        print tail(job_output_file(job_id, "stdout"), n=10)
        print "=" * 50

def print_job_metadata(job_id):
    meta = exec_engine_api.get_metadata(job_id).json()
    pprint(meta)

def print_all_condor_job_info(job_id, condor_jobs_info):
    job_info = condor_jobs_info[job_id]
    for k, v in job_info.items():
        print k, v


def adhoc(job_ids, condor_jobs_info):

    #for jid, info in condor_jobs_info.items():
    #    print jid, info

    #print_failed_job_config_path(job_configs)

    # A place for non-cli, adhoc queries
    for job_id in job_ids:
        #print_job_metadata(job_id)
        #print_tail_running_jobs(job_id, condor_jobs_info)
        #print job_output_file(job_id, "Seqware_Sanger_Somatic_Workflow.log")
        #print_failed_job_stderr(job_id)
        events = cromwell_events(job_id)
        print time_taken(job_id, events)
        pass

################################################################

class Job(object):
    def __init__(self, id, condor_info):
        self.id = id
        if condor_info is not None:
            self.condor_info = condor_info
        else:
            self.condor_info = {}

    @property
    def short_id(self):
        return self.id[:8]

    @property
    def status(self):
        return self.condor_info.get('JobStatus', 'Unknown')

    @property
    def rc(self):
        return job_rc(self.id)

    @property
    def rc_time(self):
        return job_rc_time(self.id)

    @property
    def path(self):
        return job_path(self.id)

    @property
    def stdout(self):
        return job_output_file(self.id, "stdout")

    @property
    def stderr(self):
        return job_output_file(self.id, "stderr")

    @property
    def stdout_tail(self):
        return tail(self.stdout)

    @property
    def stderr_tail(self):
        return tail(self.stderr)

    @property
    def time(self):
        return time_taken(self.id, cromwell_events(self.id))



def easy_cli(query, job_ids, condor_jobs_info):
    format_str = ' '.join('{' + q + '}' for q in query)

    for job_id in job_ids:
        job = Job(job_id, condor_jobs_info.get(job_id))

        if args.only_finished and job.status != "Completed":
            continue

        data = {k: getattr(job, k, '') for k in query}
        data.update({
            'sep': '\n' + ('=' * 80),
            'nl': '\n',
        })
        
        print format_str.format(**data)

    if args.status_count:
        print_job_status_summary(job_ids, condor_jobs_info)


################################################################

if __name__ == "__main__":

    if not socket.gethostname().startswith("application-0-1"):
        print "This script needs to be run on application-0-1"
        sys.exit(1)

    args = argparser.parse_args()
    job_configs = load_job_configs(args.jobs)
    job_ids = job_configs.keys()
    condor_jobs_info = get_condor_info_by_job()
    easy_cli(args.query, job_ids, condor_jobs_info)
    #adhoc(job_ids, condor_jobs_info)
