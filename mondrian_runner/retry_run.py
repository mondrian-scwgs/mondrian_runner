import json
import logging
import os
import random
import subprocess

import time


class IncorrectInput(Exception):
    pass


class LsfRunner(object):
    def __init__(
            self,
            working_dir,
            job_script,
            job_name_suffix,
            num_cores,
            walltime_hours,
            memory_gb,
            lsf_extra_args,
            retries=3,
            multiplier=2,
            max_memory_gb=450,
            kill_hung_jobs=False
    ):
        self.working_dir = working_dir
        self.job_script = job_script
        self.job_name_suffix = job_name_suffix
        self.num_cores = num_cores
        self.walltime_hours = walltime_hours
        self.memory_gb = memory_gb
        self.lsf_extra_args = lsf_extra_args
        self.retries = retries
        self.multiplier = multiplier
        self.max_memory_gb = max_memory_gb
        self.kill_hung_jobs = kill_hung_jobs

        self.execution_dir = os.path.join(self.working_dir, 'execution')
        self._check_working_dirs()

    def _check_working_dirs(self):
        if not os.path.exists(self.working_dir):
            IncorrectInput('missing dir: {}'.format(self.working_dir))
        if not os.path.exists(self.execution_dir):
            IncorrectInput('missing dir: {}'.format(self.execution_dir))

    def _get_job_log_files(self, job_name, init=False):
        stderr = os.path.join(self.execution_dir, '{}.stderr'.format(job_name))
        stdout = os.path.join(self.execution_dir, '{}.stdout'.format(job_name))

        if init:
            # just in case files from old runs exist
            if os.path.exists(stderr):
                os.remove(stderr)
            if os.path.exists(stdout):
                os.remove(stdout)

        return stderr, stdout

    def get_lsf_command(
            self, job_name, walltime_hours, memory_gb
    ):

        stderr, stdout = self._get_job_log_files(job_name, init=True)

        cmd = ['bsub']
        if self.lsf_extra_args is not None:
            cmd.extend(self.lsf_extra_args.split())
        cmd += [
            '-n', self.num_cores,
            '-W', '{}:00'.format(walltime_hours),
            '-R', 'rusage[mem={}]span[ptile={}]'.format(memory_gb, self.num_cores),
            '-J', job_name,
            '-cwd', self.working_dir,
            '-o', stdout,
            '-e', stderr,
            self.job_script
        ]
        return cmd

    def _parse_reason(self, reason):
        if 'TERM_MEMLIMIT' in reason:
            return 'memory'
        elif 'TERM_RUNLIMIT' in reason:
            return 'walltime'
        else:
            return 'other'

    def _get_exit_code_reason(self, job_id):
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

        reason = self._parse_reason(reason)

        return exit_code, reason

    def _check_file_keyword(self, filepath):
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

    def check_logs(self, job_name, job_id, exit_code, reason):
        logging.info('job {} completed, error code: {}'.format(job_id, exit_code))

        if exit_code == 0:
            logging.info('job {} completed successfully'.format(job_id))
            stderr, stdout = self._get_job_log_files(job_name)
            if self._check_file_keyword(stderr) or self._check_file_keyword(stdout):
                logging.warning('failure detected in log files of successful job {}'.format(job_id))
        else:
            logging.warning('job {} completed with errors. code: {}, reason:{}'.format(job_id, exit_code, reason))

    def submit_job(self, command):
        command = [str(v) for v in command]
        stdout = subprocess.check_output(command).decode()
        job_id = stdout.rstrip().replace("<", "\t").replace(">", '\t').split('\t')[1]
        logging.info('job {} submitted'.format(job_id))
        return job_id

    def _is_mem_usage_high(self, job_id, requested_mem):
        cmd = ['bjobs', '-o', 'AVG_MEM:50 MAX_MEM:50', '-json', job_id]
        stdout = subprocess.check_output(cmd).decode()
        stdout = json.loads(stdout)

        assert stdout['JOBS'] == 1
        record = stdout['RECORDS'][0]

        if 'ERROR' in record:
            raise Exception()

        max_mem = record['MAX_MEM']
        assert max_mem.endswith('Gbytes')
        max_mem = max_mem.replace(' Gbytes', '')
        max_mem = float(max_mem)

        avg_mem = record['AVG_MEM']
        assert avg_mem.endswith('Gbytes')
        avg_mem = avg_mem.replace(' Gbytes', '')
        avg_mem = float(avg_mem)

        if max_mem >= requested_mem and avg_mem / requested_mem > 0.95:
            logging.warning('job {} has exhausted requested memory'.format(job_id))
            return True

    def kill_job(self, job_id):
        cmd = ['bkill', job_id]
        stdout = subprocess.check_output(cmd).decode()
        logging.info('killing job id: {}'.format(job_id))
        logging.info(stdout)

    def monitor(self, job_id, memory, max_sleep_secs=180, min_sleep_secs=60, pend_sleep_mins=20,
                memory_monitor_mins=120):
        start_time = time.time()

        while True:
            cmd = ['bjobs', '-o', 'STAT:6', '-json', job_id]
            stdout = subprocess.check_output(cmd).decode()
            stdout = json.loads(stdout)
            assert stdout['JOBS'] == 1
            record = stdout['RECORDS'][0]

            if 'ERROR' in record:
                raise Exception()

            status = record['STAT']

            if status in ['PEND', 'WAIT', 'PROV']:
                time.sleep(pend_sleep_mins * 60)
            elif status == 'RUN':
                time.sleep(random.randint(min_sleep_secs, max_sleep_secs))
                elapsed = (time.time() - start_time) / 60
                if elapsed > memory_monitor_mins:
                    start_time = time.time()
                    if self._is_mem_usage_high(job_id, memory) and self.kill_hung_jobs:
                        self.kill_job(job_id)
            elif status in ['DONE', 'EXIT', 'UNKWN', 'ZOMBI'] or "SUSP" in status:
                exit_code, reason = self._get_exit_code_reason(job_id)
                return exit_code, reason
            else:
                raise Exception('Unknown job status: {}'.format(status))

    def run_with_lsf(self):
        memory = self.memory_gb
        walltime = self.walltime_hours

        for attempt_num in range(self.retries):
            attempt_job_name = 'attempt_{}_{}'.format(attempt_num, self.job_name_suffix)
            logging.info(
                "attempt number {} with job name {} and memory {}".format(attempt_num, attempt_job_name, memory)
            )

            command = self.get_lsf_command(
                attempt_job_name, walltime, memory
            )

            job_id = self.submit_job(command)

            exit_code, reason = self.monitor(job_id, memory)

            self.check_logs(attempt_job_name, job_id, exit_code, reason)

            if exit_code == 0:
                break

            if reason == 'walltime':
                walltime = walltime * self.multiplier
            else:
                memory = memory * self.multiplier
                memory = min(self.max_memory_gb, memory)


def update_cromwell_script(working_dir, docker_working_dir):
    scriptfile = os.path.join(working_dir, 'execution', 'script')
    with open(scriptfile, 'rt') as reader:
        data = reader.readlines()
        assert data[-1].startswith('mv') and data[-1].strip().endswith('rc')
        data = data[:-1]

    scriptfile = os.path.join(working_dir, 'execution', 'updated_script')
    with open(scriptfile, 'wt') as writer:
        for line in data:
            writer.write(line)

    docker_scriptfile = os.path.join(docker_working_dir, 'execution', 'updated_script')
    return docker_scriptfile


def get_run_script(cwd, docker_cwd, bind_mounts, singularity_img, env_setup_command):
    scriptfile = update_cromwell_script(cwd, docker_cwd)

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

    logging.info("writing runner script to {}".format(run_script))
    return run_script


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
        max_mem=450,
        kill_hung_jobs=False
):
    assert walltime.endswith(':00')
    walltime_hours = int(walltime.split(':')[0])

    run_script = get_run_script(cwd, docker_cwd, bind_mounts, singularity_img, env_setup_command)

    runner = LsfRunner(
        cwd,
        run_script,
        job_name,
        cores,
        walltime_hours,
        memory_gb,
        lsf_extra_args,
        retries=retries,
        multiplier=multiplier,
        max_memory_gb=max_mem,
        kill_hung_jobs=kill_hung_jobs
    )
    runner.run_with_lsf()

    tmp_rc_file = os.path.join(cwd, 'execution', 'rc.tmp')
    final_rc_file = os.path.join(cwd, 'execution', 'rc')
    os.rename(tmp_rc_file, final_rc_file)
