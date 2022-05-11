import json
import os
import random
import subprocess


def _get_working_dir(job_id):
    cmd = ['bjobs', '-o', 'EXEC_CWD:4096', '-json', job_id]
    stdout = subprocess.check_output(cmd).decode()
    stdout = json.loads(stdout)

    assert stdout['JOBS'] == 1
    record = stdout['RECORDS'][0]

    if 'ERROR' in record:
        raise Exception()

    return record['EXEC_CWD']


def create_rc_file_on_fail(job_id):
    working_dir = _get_working_dir(job_id)
    rcfile = os.path.join(working_dir, 'execution', 'rc')

    if os.path.exists(rcfile):
        return

    with open(rcfile, 'wt') as writer:
        writer.write('-1')


def kill_job(job_id):
    cmd = ['bkill', job_id]
    # logging.info('killing job id: {}'.format(job_id))
    stdout = subprocess.check_output(cmd).decode()
    print(stdout)

    create_rc_file_on_fail(job_id)


def _is_mem_usage_high(job_id):
    cmd = ['bjobs', '-o', 'AVG_MEM:15 MAX_MEM:15 MEMLIMIT:15 SLOTS:10', '-json', job_id]
    stdout = subprocess.check_output(cmd).decode()
    stdout = json.loads(stdout)

    assert stdout['JOBS'] == 1
    record = stdout['RECORDS'][0]

    if 'ERROR' in record:
        raise Exception()

    max_mem = record['MAX_MEM']
    if max_mem == "":
        return
    assert max_mem.endswith('Gbytes'), max_mem
    max_mem = max_mem.replace(' Gbytes', '')
    max_mem = float(max_mem)

    avg_mem = record['AVG_MEM']
    if avg_mem == "":
        return
    assert avg_mem.endswith('Gbytes'), max_mem
    avg_mem = avg_mem.replace(' Gbytes', '')
    avg_mem = float(avg_mem)

    requested_mem = record['MEMLIMIT']
    if requested_mem == "":
        return
    assert requested_mem.endswith('G'), max_mem
    requested_mem = requested_mem.replace(' G', '')
    requested_mem = float(requested_mem)

    cpu = record['SLOTS']
    if cpu == "":
        return
    cpu = int(cpu)
    requested_mem = requested_mem * cpu

    if max_mem >= requested_mem:
        if avg_mem == requested_mem - 1:
            return True
        if avg_mem / requested_mem >= 0.9:
            return True


def get_job_status(job_id):
    cmd = ['bjobs', '-o', 'STAT:6', '-json', job_id]
    stdout = subprocess.check_output(cmd).decode()
    stdout = json.loads(stdout)
    assert stdout['JOBS'] == 1
    record = stdout['RECORDS'][0]
    if 'ERROR' in record:
        raise Exception()
    status = record['STAT']

    return status


def check_alive(job_id, kill_hung_jobs=False):
    status = get_job_status(job_id)

    if status in ['PEND', 'WAIT', 'PROV', 'RUN']:
        print(status)

    # 1 in 5 chance that we query and kill job
    # to lower load on LSF
    check_hung = random.randint(1, 5) == 5
    if kill_hung_jobs and status == 'RUN' and check_hung:
        if _is_mem_usage_high(job_id):
            kill_job(job_id)
            return

    # if we print nothing, cromwell assumes job finished
    if 'SUSP' in status or status == 'EXIT':
        create_rc_file_on_fail(job_id)
