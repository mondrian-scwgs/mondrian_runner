import errno
import json
import logging
import os
import random
import shutil
from functools import wraps
from subprocess import Popen, PIPE

import time
import yaml

import mondrianutils.helpers


class Backoff(object):
    """
    wrapper for functions that fail but require retries until it succeeds
    """

    def __init__(self, exception_type=Exception, max_backoff=3600, backoff_time=1, randomize=False,
                 num_retries=None, backoff="exponential", step_size=2):
        self.func = None

        if backoff not in ["exponential", "linear", "fixed"]:
            raise Exception(
                "Currently supports only exponential, linear and fixed backoff")

        self.exception_type = exception_type

        self.max_backoff = max_backoff
        self.backoff_time = backoff_time

        self.randomize = randomize

        self.num_retries = num_retries

        self.backoff = backoff

        self.step_size = step_size

        self.elapsed_time = 0

    def __call__(self, func):
        self.func = func

        @wraps(func)
        def wrapped(*args, **kwargs):
            return self._run_with_exponential_backoff(*args, **kwargs)

        return wrapped

    def _run_with_exponential_backoff(self, *args, **kwargs):
        """
        keep running the function until we go over the
        max wait time or num retries
        """

        retry_no = 0

        while True:

            if self.elapsed_time >= self.max_backoff:
                break

            if self.num_retries and retry_no > self.num_retries:
                break

            try:
                result = self.func(*args, **kwargs)
            except self.exception_type as exc:
                self._update_backoff_time()
                logging.getLogger("pypeliner.helpers").warn(
                    "error {} caught, retrying after {} seconds".format(
                        str(exc), self.backoff_time)
                )
                retry_no += 1
                time.sleep(self.backoff_time)
            except Exception:
                raise
            else:
                return result

    def _update_backoff_time(self):
        """
        update the backoff time
        """

        if self.backoff == "exponential":
            self.backoff_time = self.step_size * (self.backoff_time or 1)

        elif self.backoff == "linear":
            self.backoff_time += self.step_size

        if self.randomize:
            lower_bound = int(0.9 * self.backoff_time)
            upper_bound = int(1.1 * self.backoff_time)
            self.backoff_time = random.randint(lower_bound, upper_bound)

        if self.elapsed_time + self.backoff_time > self.max_backoff:
            self.backoff_time = self.max_backoff - self.elapsed_time

        self.elapsed_time += self.backoff_time


def get_incrementing_filename(path):
    """
    avoid overwriting files. if path exists then return path
    otherwise generate a path that doesnt exist.
    """

    if not os.path.exists(path):
        return path

    i = 0
    while os.path.exists("{}.{}".format(path, i)):
        i += 1

    return "{}.{}".format(path, i)


def init_log_file(logfile):
    if os.path.exists(logfile):
        newpath = get_incrementing_filename(logfile)
        os.rename(logfile, newpath)


def init_logger(tempdir, loglevel):
    logfile = os.path.join(tempdir, 'pipeline.log')
    logfile = init_log_file(logfile)

    logging.basicConfig(
        level=logging.DEBUG,
        filename=logfile,
        filemode='wt',
        format='%(name)s - %(levelname)s - %(message)s'
    )
    console = logging.StreamHandler()
    console.setLevel(loglevel)
    logging.getLogger('').addHandler(console)


def get_id_from_tempdir(tempdir):
    idfile = os.path.join(tempdir, 'run_id.txt')

    with open(idfile, 'rt') as reader:
        data = reader.readlines()

        assert len(data) == 1
        data = data[0].strip()

    return data


def cache_run_id(run_id, outdir):
    id_file = os.path.join(outdir, 'run_id.txt')
    with open(id_file, 'wt') as writer:
        writer.write(run_id)


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


def get_workflow_url(wf_name, version):
    wf_url = 'https://raw.githubusercontent.com/mondrian-scwgs/mondrian/{}/mondrian/wdl/analyses/{}.wdl'.format(
        version, wf_name)
    return wf_url


def get_run_id(stdout):
    run_id = json.loads(stdout)['id']

    return run_id


def check_status(server_url, run_id, logger):
    i = 0
    while i < 5:

        cmd = ['curl', '-X', 'GET', "http://{}/api/workflows/v1/query?id={}".format(server_url, run_id)]

        cmdout, cmderr = run_cmd(cmd)

        cmdout = json.loads(cmdout)

        if 'results' not in cmdout:
            logger.warning(f'expected results in response, received {cmdout}')
            i += 1
            time.sleep(20)
            continue

        if not len(cmdout['results']) == 1:
            logger.warning('expected 1 result. {}'.format(cmdout['results']))
            i += 1
            time.sleep(20)
            continue

        status = cmdout['results'][0]['status'].lower()

        return status


@Backoff(max_backoff=900, randomize=True)
def wait(server_url, run_id, log_file, sleep_time=30):
    logger = logging.getLogger('mondrian_runner_waiter')

    while True:
        time.sleep(sleep_time)

        if os.path.exists(log_file):
            follow_log = follow(
                log_file, server_url, run_id, logger
            )
            for line in follow_log:
                logger.info(line)

        status = check_status(server_url, run_id, logger)
        if status not in ['running', 'submitted']:
            break

    return check_status(server_url, run_id, logger)


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


def follow(logfile, server_url, run_id, logger, sleep_time=10):
    '''
    generator function that yields new lines in a file
    '''

    logreader = open(logfile, 'rt')

    logreader.seek(0, os.SEEK_END)
    while True:

        if not os.path.exists(logfile):
            break

        line = logreader.readline()
        if not line:
            status = check_status(server_url, run_id, logger)
            if status not in ['running', 'submitted']:
                break
            time.sleep(sleep_time)
            continue

        yield line


def load_options_json(options_json):
    data = json.load(open(options_json, 'rt'))

    return {
        'wf_logs': data['final_workflow_log_dir'],
        'out_dir': data['final_workflow_outputs_dir'],
    }


def extract_name_version(wdl_file):
    with open(wdl_file, 'rt') as wdl_reader:
        pipeline_version = wdl_reader.readline()

        if pipeline_version.startswith('#{"meta"'):
            pipeline_version = pipeline_version[1:]
            pipeline_version = json.loads(pipeline_version)['meta']
            return pipeline_version['name'], pipeline_version['version']
        else:
            return None, None



def get_all_outputs(outdir):
    files = os.listdir(outdir)
    for file in files:
        if os.path.isdir(file):
            raise Exception("dir found in outputdir")

    return files



def create_metadata_yaml(outdir, pipeline_wdl, yamlfile):
    name, version = extract_name_version(pipeline_wdl)
    files = get_all_outputs(outdir)

    mondrian_utils.helpers.validate_outputs(files, name)


    data = {
        'filenames': files,
        'meta': {
            'version': version,
            'name': name
        }
    }

    with open(yamlfile, 'wt') as writer:
        yaml.dump(data, writer, default_flow_style=False)


def add_metadata(options_json, input_json, pipeline_wdl):
    options_data = load_options_json(options_json)

    out_dir = options_data['out_dir']

    shutil.copyfile(input_json, os.path.join(out_dir, "input.json"))

    create_metadata_yaml(out_dir, pipeline_wdl, os.path.join(out_dir, "metadata.yaml"))
