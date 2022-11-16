import errno
import glob
import json
import logging
import os
import random
from subprocess import Popen, PIPE

import time


def submit_pipeline(server_url, wdl_file, input_json=None, options_json=None, imports=None):
    logger = logging.getLogger('mondrian_runner.submit')

    cmd = [
        'curl',
        '-X', 'POST',
        '--header', 'Accept: application/json',
        '-v', '{}/api/workflows/v1'.format(server_url),
        '-F', 'workflowSource=@{}'.format(wdl_file),
    ]

    if input_json is not None:
        cmd += ['-F', 'workflowInputs=@{}'.format(input_json)]

    if options_json is not None:
        cmd += ['-F', 'workflowOptions=@{}'.format(options_json)]

    if imports is not None:
        cmd += ['-F', 'workflowDependencies=@{}'.format(imports)]

    logger.info('running: {}'.format(' '.join(cmd)))

    cmdout, cmderr = run_cmd(cmd)

    run_id = get_run_id(cmdout)

    logger.info("run_id: {}".format(run_id))

    return run_id


def init_console_logger(loglevel):
    logging.basicConfig(
        level=loglevel,
        format='%(name)s - %(levelname)s - %(message)s'
    )


def get_latest_id_from_cache_dir(tempdir):
    id_file = get_cache_file(tempdir)

    if not os.path.exists(id_file):
        return None

    with open(id_file, 'rt') as reader:
        return json.load(reader)['run_id']


def get_all_ids_from_cache_dir(tempdir):
    id_file = get_cache_file(tempdir)

    with open(id_file, 'rt') as reader:
        data = json.load(reader)

    return [data['run_id']] + data['old_run_ids']


def get_cache_file(cache_dir):
    cache_file = os.path.join(cache_dir, 'run_data_cache.json')
    return cache_file


def cache_run_id(run_id, cache_dir):
    id_file = get_cache_file(cache_dir)

    if os.path.exists(id_file):
        with open(id_file, 'rt') as reader:
            data = json.load(reader)
        data['old_run_ids'] = [data['run_id']] + data['old_run_ids']
        data['run_id'] = run_id
    else:
        data = {
            'run_id': run_id,
            'old_run_ids': []
        }
    with open(id_file, 'wt') as writer:
        json.dump(data, writer)


def run_cmd(cmd):
    """
    run command with subprocess,
    write stdout to output file if set
    :param cmd: command args
    :type cmd: list of str
    :param output: filepath for stdout
    :type output: str
    """
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)

    cmdout, cmderr = p.communicate()

    retc = p.returncode

    if retc:
        raise Exception(
            "command failed. stderr:{}, stdout:{}".format(
                cmdout,
                cmderr))

    return cmdout, cmderr


def run_cmd_interactive(cmd):
    """
    run command with subprocess,
    write stdout to output file if set
    :param cmd: command args
    :type cmd: list of str
    :param output: filepath for stdout
    :type output: str
    """
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)

    for stdout_line in iter(p.stdout.readline, ""):
        yield stdout_line

    p.stdout.close()
    retc = p.wait()

    if retc:
        raise Exception(f"command {cmd} failed.")


def get_run_id(stdout):
    try:
        run_id = json.loads(stdout)['id']
    except:
        raise Exception('unable to parse id: {}'.format(stdout))

    return run_id


def check_status(server_url, run_id, num_retries=0):
    i = 0
    while i <= num_retries:
        cmd = ['curl', '-X', 'GET', "http://{}/api/workflows/v1/query?id={}".format(server_url, run_id)]

        cmdout, cmderr = run_cmd(cmd)

        cmdout = json.loads(cmdout)

        if 'results' not in cmdout:
            logging.getLogger('mondrian_runner.poll').warning(f'expected results in response, received {cmdout}')
            i += 1
            time.sleep(20)
            continue

        if not len(cmdout['results']) == 1:
            logging.getLogger('mondrian_runner.poll').warning('expected 1 result. {}'.format(cmdout['results']))
            i += 1
            time.sleep(20)
            continue

        status = cmdout['results'][0]['status'].lower()

        return status


def makedirs(directory):
    """
    make a dir if it doesnt exist
    :param directory: dir path
    :type directory: str
    """
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def get_wf_name(execution_dir, run_id):
    paths = glob.glob('{}/*/{}'.format(execution_dir, run_id))

    assert len(paths) == 1

    paths = paths[0]

    paths = paths.replace(execution_dir, '')
    paths = paths.replace(run_id, '')
    paths = paths.replace('/', '')

    return paths


def get_wf_name_from_input_json(json_file):
    with open(json_file, 'rt') as reader:
        data = json.load(reader)

    keys = data.keys()
    keys = [v.split('.')[0] for v in keys]
    keys = sorted(set(keys))

    assert len(keys) == 1

    return keys[0]


class PipelineLockedError(Exception):
    pass


class PipelineLock(object):
    def __init__(self, cache_dir):
        self.lock = os.path.join(cache_dir, '_lock')

    def __enter__(self):
        if os.path.exists(self.lock):
            raise PipelineLockedError(f'pipeline already running. remove {self.lock} to override')

        try:
            os.makedirs(self.lock)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise PipelineLockedError(f'Pipeline already running, remove {self.lock} to override')
            else:
                raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.rmdir(self.lock)


def _simple_wait_and_log(server_url, run_id, workflow_log_dir, sleep_time=30):
    log_file = os.path.join(workflow_log_dir, 'workflow.{}.log'.format(run_id))
    logger = logging.getLogger('mondrian_runner.poll')

    if not os.path.exists(log_file):
        return check_status(server_url, run_id, num_retries=4)

    logreader = open(log_file, 'rt')
    logreader.seek(0, os.SEEK_END)

    status = None
    while os.path.exists(log_file) or status in ['running', 'submitted']:
        line = logreader.readline()
        if line:
            logger.info(line.strip())
        else:
            status = check_status(server_url, run_id, num_retries=4)
            time.sleep(sleep_time)

    return check_status(server_url, run_id, num_retries=4)


def wait(server_url, run_id, workflow_log_dir, sleep_time=30):
    x = 0
    num_retries = 5
    backoff_in_seconds = 10

    while True:
        try:
            status = _simple_wait_and_log(server_url, run_id, workflow_log_dir, sleep_time=sleep_time)
            if status not in ['running', 'submitted']:
                return status
        except KeyboardInterrupt:
            if x == num_retries - 1:
                raise
            else:
                sleep = (backoff_in_seconds * 2 ** x + random.uniform(0, 1))
                logging.getLogger('mondrian_runner.poll').info(f'Exception caught, retrying after {sleep} seconds')
                time.sleep(sleep)
                x += 1
