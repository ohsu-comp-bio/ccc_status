"""
TODO: this only handles workflows with a single call currently.
      this affects the cromwell/condor job status codes.
"""

import argparse
from collections import Counter, namedtuple
from datetime import datetime, timedelta
import json
import os
from pprint import pprint
import socket
import subprocess
import sys
import time
import types
import xml.etree.ElementTree as ET

import ccc_client

epilog="""
Available query keys:
  id          : Short ID e.g. abcd0983
  full_id     : Full UUID
  status      : Condor status
  rc          : Return code
  rc_time     : Timestamp on the rc file
  path        : Filesystem path to the Cromwell execution directory
  stdout      : Full stdout content
  stderr      : Full stderr content
  stdout_tail : Last 20 lines of stdout
  stderr_tail : Last 20 lines of stderr
  time        : Time taken from start to finish, calculated from Cromwell events.
  condor_id   : Condor job ID.
  condor_meta : Dump all Condor job metadata.
  sep         : Line separator, useful when dumping stdout for many jobs.
  nl          : Newline.
"""

argparser = argparse.ArgumentParser(
    usage="%(prog)s job-info.json... --query key...",
    epilog=epilog,
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
argparser.add_argument("--summary", "-s", action="store_true")
argparser.add_argument("--only-finished", "-f", action="store_true")
argparser.add_argument("--query", "-q", nargs="+")
argparser.add_argument("jobs", nargs="+")

exec_engine_api = ccc_client.ExecEngineRunner()

CONDOR_STATUS_CODES = {
    "0": "Unexpanded",
    "1": "Idle",
    "2": "Running",
    "3": "Removed",
    "4": "Completed",
    "5": "Held",
    "6": "Submission_err",
}

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

def tail(string, n=20):
    lines = string.split("\n")
    return "\n".join(lines[-n:])


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


def print_job_status_summary(job_ids, condor_jobs_info):
    counts = Counter()
    for job_id in job_ids:
        job_info = condor_jobs_info[job_id]
        job_status = job_info['JobStatus']
        counts[job_status] += 1

    print counts


################################################################

class Call(object):
    def __init__(self, name, id, condor_info, exec_dir):
        self.name = name
        self.id = id
        self.condor_info = condor_info or {}
        self.exec_dir = exec_dir

    def status(self):
        return self.condor_info.get('JobStatus', 'Unknown')

    def path(self, *args):
        return os.path.join(self.exec_dir, *args)

    def rc(self):
        rc_path = self.path("rc")
        has_rc = os.path.exists(rc_path)

        if has_rc:
            with open(rc_path) as fh:
                return int(fh.read().strip())

    def rc_time(self):
        rc_path = self.path("rc")
        has_rc = os.path.exists(rc_path)

        if has_rc:
            mtime = os.path.getmtime(rc_path)
            return time.strftime("%a, %d %b %Y %H:%M:%S", time.gmtime(mtime))

    def output_file(self, file_name):
        p = self.path(file_name)
        with open(p) as fh:
            return fh.read()

    def failed(self):
        return self.rc() is not None and rc != 0

    def stdout(self):
        return self.output_file("stdout")

    def stderr(self):
        return self.output_file("stderr")

    def stdout_tail(self):
        return tail(self.stdout())

    def stderr_tail(self):
        return tail(self.stderr())

    def events(self):
        log_content = self.output_file(self.name + '.log')
        # The ccc/cromwell output doesn't have a proper root element,
        # so give it one, which makes parsing easier.
        log_content = '<root>' + log_content + '</root>'
        events = parse_event_log(log_content)
        return events

    def time(self):
        parse_format = '%Y-%m-%dT%H:%M:%S'

        try:
            events = self.events()
        except IOError:
            return "No event log"

        submit_event = None
        exec_event = None
        end_event = None

        for event in events:
            if event['MyType'] == 'SubmitEvent':
                submit_event = event
            elif event['MyType'] == 'ExecuteEvent':
                exec_event = event
            elif event['MyType'] == 'JobTerminatedEvent':
                end_event = event

        if not exec_event:
            return

        if not end_event:
            end_time = datetime.now()
        else:
            end_time = datetime.strptime(end_event['EventTime'], parse_format)

        start_time = datetime.strptime(exec_event['EventTime'], parse_format)
        return end_time - start_time

    def days(self):
        delta = self.time()
        if isinstance(delta, timedelta):
            hours = float(delta.seconds) / 60 / 60
            days = delta.days + hours / 24
            return '{days:.1f} days'.format(days=days)
        else:
            return delta

    def condor_id(self):
        return self.condor_info.get('ClusterId', 'Unknown')

    def condor_meta(self):
        fmt = '{0:<25} = {1}'.format
        items = self.condor_info.items()
        return '\n'.join(fmt(k, v) for k, v in items)



def detect_column_widths(rows):
    widths = [0 for i in xrange(len(rows[0]))]
    for row in rows:
        for i, col in enumerate(row):
            width = len(col)
            # Don't bother with wide columns
            if width < 40:
                widths[i] = max(widths[i], len(col))
    return widths

def format_output_table(rows):
    string_rows = []
    for row in rows:
        string_rows.append([str(col) for col in row])

    widths = detect_column_widths(string_rows)
    output = ''
    for row in string_rows:
        output += ' '.join(col.ljust(widths[i]) for i, col in enumerate(row))
        output += '\n'

    return output


def easy_cli(query, job_ids, condor_jobs_info):
    if query:
        common = {
            'sep': '\n' + ('=' * 80),
            'nl': '\n',
        }

        rows = []
        for job_id in job_ids:
            for meta in get_ccc_calls_metadata(job_id):
                cols = []
                rows.append(cols)
                call = Call(meta.name, meta.id, condor_jobs_info.get(job_id),
                            meta.exec_dir)

                if args.only_finished and call.status() != "Completed":
                    continue

                for q in query:
                    if q in common:
                        col = common[q]
                    elif hasattr(call, q):
                        col = getattr(call, q)
                        if isinstance(col, types.MethodType):
                            col = col()
                    else:
                        raise Exception("Unknown query key: {}".format(q))

                    cols.append(col)
            
        print format_output_table(rows)

    if args.summary:
        print_job_status_summary(job_ids, condor_jobs_info)


################################################################

CCC_CallMeta = namedtuple('CCC_CallMeta', 'name id exec_dir')

def get_ccc_calls_metadata(job_id):
    EXEC_DIR = "/cluster_share/cromwell-executions"

    calls = []
    meta = exec_engine_api.get_metadata(job_id).json()
    workflow_name = meta['workflowName']

    for call in meta['calls'].keys():
        call_name = call.split('.')[-1]
        call_dir_name = 'call-' + call_name
        exec_dir = os.path.join(EXEC_DIR, workflow_name, job_id, call_dir_name)
        call_id = job_id[:8] + ':' + call_name
        call_meta = CCC_CallMeta(call_name, call_id, exec_dir)
        calls.append(call_meta)

    return calls


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
