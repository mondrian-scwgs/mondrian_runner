import json
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


def get_multiplier(cwd, multiplier):
    attempt = cwd.split('/')[-1]
    if attempt.startswith('attempt-'):
        attempt = int(attempt[len('attempt-'):]) - 1
        return attempt * multiplier
    else:
        return 1


def get_prev_cwd(cwd):
    attempt = cwd.split('/')[-1]
    assert attempt.startswith('attempt-')
    attempt = int(attempt[len('attempt-'):])

    root_cwd = '/'.join(cwd.split('/')[:-1])

    if attempt == 2:
        return root_cwd
    else:
        return '{}/attempt-{}'.format(root_cwd, attempt - 1)


def find_job_id(cwd):
    submit_stdout = "{}/execution/stdout.submit".format(cwd)
    with open(submit_stdout, 'rt') as reader:
        data = reader.readlines()
        assert len(data) == 1
        data = data[0]
    jobid = data.split('<')[1].split('>')[0]
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


def generate_bsub_command(
        cwd, multiplier, walltime, memory_gb,
        cpu, job_name, out, err, docker_cwd,
        singularity_img, job_shell, docker_script,
        max_mem=None, max_walltime_hrs=None,
        bind_mounts=None, lsf_extra_args=None
):
    multiplier = get_multiplier(cwd, multiplier)

    if multiplier == 1:
        submit_job(
            cpu, walltime, memory_gb, job_name,
            cwd, out, err, lsf_extra_args,
            docker_cwd, bind_mounts,
            singularity_img, job_shell,
            docker_script
        )
        return

    prev_cwd = get_prev_cwd(cwd)
    prev_job_id = find_job_id(prev_cwd)
    fail_reason = get_job_failure_reason(prev_job_id)

    if 'TERM_RUNLIMIT' in fail_reason:
        walltime = update_walltime(
            walltime, multiplier, max_walltime_hrs=max_walltime_hrs
        )
    else:
        memory_gb = update_memory(
            memory_gb, cpu, multiplier, max_mem=max_mem
        )

    submit_job(
        cpu, walltime, memory_gb, job_name,
        cwd, out, err, lsf_extra_args,
        docker_cwd, bind_mounts,
        singularity_img, job_shell,
        docker_script
    )
