import json
import logging
import os
import random
import subprocess

import time

"""
TODO
 x logging
 x max memory (so we dont submit jobs that would never complete)
 x retry with more walltime
 x detect job failure reason and decide whether to increase walltime or mem
 - mem usage monitoring for that bug
 x bsub args for queue info etc
 x get exit code, add  EXIT_CODE:5
 x delete files from old runs 
 - handle module load singularity better
"""


def submit_job(command):
    command = [str(v) for v in command]
    stdout = subprocess.check_output(command).decode()
    job_id = stdout.rstrip().replace("<", "\t").replace(">", '\t').split('\t')[1]
    return job_id


def parse_reason(reason):
    if 'TERM_MEMLIMIT' in reason:
        return 'memory'
    elif 'TERM_RUNLIMIT' in reason:
        return 'walltime'
    else:
        return 'other'


def get_exit_code(job_id):
    cmd = ['bjobs', '-o', 'EXIT_CODE:6 EXIT_REASON:50', '-json', job_id]
    stdout = subprocess.check_output(cmd).decode()
    stdout = json.loads(stdout)

    assert stdout['JOBS'] == 1
    record = stdout['RECORDS'][0]

    if 'ERROR' in record:
        raise Exception()

    exit_code = record['EXIT_CODE']
    if exit_code == "":
        exit_code = 0
    reason = record['EXIT_REASON']

    return exit_code, reason


def monitor(job_id, max_sleep=180, min_sleep=60):
    while True:
        cmd = ['bjobs', '-o', 'STAT:6', '-json', job_id]
        stdout = subprocess.check_output(cmd).decode()
        stdout = json.loads(stdout)
        assert stdout['JOBS'] == 1
        record = stdout['RECORDS'][0]

        if 'ERROR' in record:
            raise Exception()

        status = record['STAT']

        if status in ['DONE', 'EXIT', 'UNKWN', 'ZOMBI'] or "SUSP" in status:
            exit_code, reason = get_exit_code(job_id)
            return exit_code, reason
        elif status in ['PEND', 'WAIT', 'PROV', 'RUN']:
            time.sleep(random.randint(min_sleep, max_sleep))
        else:
            raise Exception('Unknown job status: {}'.format(status))


def _check_file_keyword(filepath):
    err_found = False
    with open(filepath, 'rt') as reader:
        for line in reader:
            if 'err' in line.lower():
                err_found = True
            elif 'terminate' in line.lower():
                err_found = True
            elif 'killed' in line.lower():
                err_found = True
    return err_found


def check_logs(cwd, job_name):
    stderr = os.path.join(cwd, 'execution', '{}.stderr'.format(job_name))
    stdout = os.path.join(cwd, 'execution', '{}.stdout'.format(job_name))
    return _check_file_keyword(stderr) or _check_file_keyword(stdout)


def lsf_runner(command, cwd, job_name):
    job_id = submit_job(command)

    logging.info('job {} submitted'.format(job_id))

    exit_code, reason = monitor(job_id)
    reason = parse_reason(reason)

    logging.info('job {} completed, error code: {}'.format(job_id, exit_code))
    if exit_code == 0:
        logging.info('job {} completed successfully'.format(job_id))
    else:
        logging.info('job {} completed with errors. code: {}, reason:{}'.format(job_id, exit_code, reason))

    # double check if no error
    if exit_code == 0 and check_logs(cwd, job_name):
        logging.warning('failure detected in log files of successful job {}'.format(job_id))

    return exit_code, reason


def get_run_script(cwd, docker_cwd, bind_mounts, singularity_img, env_setup_command):

    scriptfile = os.path.join(cwd, 'execution', 'script')
    with open(scriptfile, 'rt') as reader:
        data = reader.readlines()
        assert data[-1].startswith('mv') and data[-1].strip().endswith('rc')
        data = data[:-1]
    scriptfile = os.path.join(cwd, 'execution', 'updated_script')
    with open(scriptfile, 'wt') as writer:
        for line in data:
            writer.write(line)

    scriptfile = os.path.join(docker_cwd, 'execution', 'updated_script')

    cmd = [
        'singularity', 'exec', '--containall',
        '--bind', '{}:{}'.format(cwd, docker_cwd)
    ]
    for mount in bind_mounts:
        cmd.extend(['--bind', mount])
    cmd.append(singularity_img)
    cmd.extend(['/bin/bash', scriptfile])

    cmd = ' '.join(cmd)

    run_script = os.path.join(cwd, 'execution', 'run_script.sh')
    with open(run_script, 'wt') as writer:
        if env_setup_command is not None:
            writer.write(env_setup_command + '\n')
        writer.write(cmd + '\n')

    os.chmod(run_script, 0o777)

    return run_script


def get_lsf_command(cwd, job_name, num_cores, walltime_hours, memory_gb, run_script, lsf_extra_args):
    stderr = os.path.join(cwd, 'execution', '{}.stderr'.format(job_name))
    stdout = os.path.join(cwd, 'execution', '{}.stdout'.format(job_name))

    # just in case files from old runs exist
    if os.path.exists(stderr):
        os.remove(stderr)
    if os.path.exists(stdout):
        os.remove(stdout)

    cmd = ['bsub']

    if lsf_extra_args is not None:
        cmd.extend(lsf_extra_args.split())

    cmd += [
        '-n', num_cores,
        '-W', '{}:00'.format(walltime_hours),
        '-R', 'rusage[mem={}]span[ptile={}]'.format(memory_gb, num_cores),
        '-J', job_name,
        '-cwd', cwd,
        '-o', stdout,
        '-e', stderr,
        run_script
    ]
    return cmd


def run_with_lsf(
        retries, cwd, job_name,
        num_cores, walltime_hours, memory_gb,
        run_script, multiplier, lsf_extra_args, max_mem=450
):
    rc = -1
    for attempt_num in range(retries):
        attempt_job_name = '{}_attempt_{}'.format(job_name, attempt_num)
        logging.info(
            "attempt number {} with job name {} and memory {}".format(attempt_num, attempt_job_name, memory_gb))

        command = get_lsf_command(
            cwd, attempt_job_name, num_cores, walltime_hours, memory_gb, run_script, lsf_extra_args
        )

        rc, reason = lsf_runner(command, cwd, attempt_job_name)

        if rc == 0:
            break

        if reason in ['memory', 'other']:
            memory_gb = memory_gb * multiplier
            memory_gb = min(max_mem, memory_gb)
        elif reason in 'walltime':
            walltime_hours = walltime_hours * multiplier

    if not rc == 0:
        raise Exception('Failed with max mem {}'.format(memory_gb))

    tmp_rc_file = os.path.join(cwd, 'execution', 'rc.tmp')
    final_rc_file = os.path.join(cwd, 'execution', 'rc')
    os.rename(tmp_rc_file, final_rc_file)


def retry_run(
        cwd,
        docker_cwd,
        singularity_img,
        env_setup_command,
        memory_gb,
        walltime,
        cores,
        retries,
        multiplier,
        job_name,
        bind_mounts,
        lsf_extra_args,
        max_mem=450
):
    assert walltime.endswith(':00')
    walltime_hours = int(walltime.split(':')[0])

    run_script = get_run_script(cwd, docker_cwd, bind_mounts, singularity_img, env_setup_command)
    logging.info("writing runner script to {}".format(run_script))

    run_with_lsf(
        retries,
        cwd,
        job_name,
        cores,
        walltime_hours,
        memory_gb,
        run_script,
        multiplier,
        lsf_extra_args,
        max_mem=max_mem
    )
