import subprocess


def generate_bsub_command(
        cwd, multiplier, walltime, memory_gb,
        cpu, job_name, out, err, docker_cwd,
        singularity_img, job_shell, docker_script,
        max_mem=None, bind_mounts=None, lsf_extra_args=None
):
    attempt_num = [v for v in cwd.split('/') if 'attempt' in v]

    if len(attempt_num) == 0:
        multiplier = 1
    else:
        assert len(attempt_num) == 1, attempt_num
        attempt_num = int(attempt_num[0].split('-')[1]) - 1
        multiplier = attempt_num * int(multiplier)

    walltime = walltime.split(':')
    assert len(walltime) == 2, walltime
    walltime[0] = int(walltime[0]) * multiplier
    walltime[1] = int(walltime[1]) * multiplier
    walltime = '{}:{:02d}'.format(walltime[0], walltime[1])
    memory_gb = int(memory_gb) * multiplier

    # this is the max available mem on the nodes we own. if job requests more
    # then queueing will take infinite time.
    if max_mem is not None and memory_gb * cpu > max_mem:
        memory_gb = max_mem // cpu

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
