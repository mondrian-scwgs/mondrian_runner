import json
import os
import subprocess


def submit_job(
        cpu, walltime, memory_gb, job_name,
        cwd, out, err, lsf_extra_args,
        docker_cwd, bind_mounts,
        singularity_img, job_shell,
        docker_script
):
    cmd = [
        "bsub", "-n", cpu, "-W", walltime,
        "-R", "'rusage[mem={}]span[ptile={}]'".format(memory_gb, cpu),
        "-J", job_name, "-cwd", cwd, "-o", out, "-e", err
    ]

    if lsf_extra_args is not None:
        cmd.extend(lsf_extra_args.split())

    cmd += [
        "--wrap", "singularity", "exec", "--containall", "--bind",
        "{}:{}".format(cwd, docker_cwd)
    ]

    for mount in bind_mounts:
        cmd.extend(['--bind', mount])

    cmd += [
        singularity_img, job_shell, docker_script
    ]

    cmd = [str(v) for v in cmd]
    stdout = subprocess.check_output(cmd).decode()
    print(stdout)

    job_id = find_job_id(stdout)
    return job_id


def is_restart(cwd):
    attempt = cwd.split('/')[-1]
    if attempt.startswith('attempt-'):
        return True
    else:
        return False


def get_prev_cwd(cwd):
    attempt = cwd.split('/')[-1]
    assert attempt.startswith('attempt-')
    attempt = int(attempt[len('attempt-'):])

    root_cwd = '/'.join(cwd.split('/')[:-1])

    if attempt == 2:
        return root_cwd
    else:
        return '{}/attempt-{}'.format(root_cwd, attempt - 1)


def find_job_id(stdout):
    jobid = stdout.split('<')[1].split('>')[0]
    return jobid


def get_job_failure_reason(job_id):
    cmd = ['bjobs', '-o', 'EXIT_REASON:50', '-json', job_id]
    stdout = subprocess.check_output(cmd).decode()
    stdout = json.loads(stdout)
    assert stdout['JOBS'] == 1
    reason = stdout['RECORDS'][0]['EXIT_REASON']
    return reason


def update_walltime(walltime, multiplier, max_walltime_hrs=None):
    hours, mins = walltime.split(':')
    hours = int(hours) * multiplier
    mins = int(mins) * multiplier
    # job time limit as imposed by the cluster
    if max_walltime_hrs is not None and hours > max_walltime_hrs:
        walltime = "{}:00".format(max_walltime_hrs)
    else:
        walltime = '{}:{:02d}'.format(hours, mins)
    return walltime


def update_memory(memory_gb, cpu, multiplier, max_mem=None):
    memory_gb = int(memory_gb) * multiplier
    # this is the max available mem on the nodes we own. if job requests more
    # then queueing will take infinite time.
    if max_mem is not None and memory_gb * cpu > max_mem:
        memory_gb = max_mem // cpu
    return memory_gb


def update_resource_requests(
        walltime, memory_gb, multiplier, cpu, fail_reason,
        max_mem=None, max_walltime_hrs=None
):
    if 'TERM_RUNLIMIT' in fail_reason:
        walltime = update_walltime(
            walltime, multiplier, max_walltime_hrs=max_walltime_hrs
        )
    else:
        memory_gb = update_memory(
            memory_gb, cpu, multiplier, max_mem=max_mem
        )

    return walltime, memory_gb


def cache_job_information(job_id, walltime, memory_gb, cwd):
    cache_file = os.path.join(cwd, 'execution', 'job_information.json')
    if os.path.exists(cache_file):
        raise Exception('Cannot cache, file exists:{}'.format(cache_file))

    with open(cache_file, 'wt') as writer:
        json.dump(
            {'job_id': job_id, 'walltime': walltime, 'memory_gb': memory_gb},
            writer
        )


def retrieve_job_information(cwd):
    cache_file = os.path.join(cwd, 'execution', 'job_information.json')
    with open(cache_file, 'rt') as reader:
        data = json.load(reader)
    return data


def generate_bsub_command(
        cwd, multiplier, walltime, memory_gb,
        cpu, job_name, out, err, docker_cwd,
        singularity_img, job_shell, docker_script,
        max_mem=None, max_walltime_hrs=None,
        bind_mounts=None, lsf_extra_args=None
):
    if not is_restart(cwd):
        job_id = submit_job(
            cpu, walltime, memory_gb, job_name,
            cwd, out, err, lsf_extra_args,
            docker_cwd, bind_mounts,
            singularity_img, job_shell,
            docker_script
        )
        cache_job_information(job_id, walltime, memory_gb, cwd)
        return

    prev_cwd = get_prev_cwd(cwd)
    prev_job_info = retrieve_job_information(prev_cwd)
    fail_reason = get_job_failure_reason(prev_job_info['job_id'])

    walltime, memory_gb = update_resource_requests(
        prev_job_info['walltime'], prev_job_info['memory_gb'], multiplier,
        cpu, fail_reason, max_walltime_hrs=max_walltime_hrs,
        max_mem=max_mem
    )

    submit_job(
        cpu, walltime, memory_gb, job_name,
        cwd, out, err, lsf_extra_args,
        docker_cwd, bind_mounts,
        singularity_img, job_shell,
        docker_script
    )
