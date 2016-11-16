# TODO these are very outdated

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
        pass
