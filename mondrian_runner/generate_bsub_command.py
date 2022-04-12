def generate_bsub_command(
        cwd, multiplier, walltime, memory_gb,
        cpu, job_name, out, err, docker_cwd,
        singularity_img, job_shell, docker_script, max_mem=450
):
    cmd = "bsub -n {cpu} -W {walltime} -R 'rusage[mem={memory_gb}]span[ptile={cpu}]' " \
          "-J {job_name} -cwd {cwd} -o {out} -e {err} --wrap singularity exec " \
          "--containall --bind /work --bind /juno/work --bind {cwd}:{docker_cwd} " \
          "{singularity_img} {job_shell} {docker_script}"

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
    if memory_gb*cpu > max_mem:
        memory_gb = max_mem//cpu

    cmd = cmd.format(
        cpu=cpu,
        walltime=walltime,
        memory_gb=memory_gb,
        job_name=job_name,
        cwd=cwd,
        out=out,
        err=err,
        docker_cwd=docker_cwd,
        singularity_img=singularity_img,
        job_shell=job_shell,
        docker_script=docker_script
    )

    print(cmd)
