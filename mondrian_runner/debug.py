import logging
import os
from collections import deque


def extract_err_from_file(filepath, max_file_length=300, num_lines_per_error=100):
    sep = '-' * 80 + '\n' + filepath + '-'*80 + '\n'

    numlines = sum([1 for _ in open(filepath)])
    if numlines < max_file_length:
        return ''.join([v for v in open(filepath, 'rt').readlines()])

    outdata = []
    buffer = deque([], maxlen=num_lines_per_error)
    with open(filepath, 'rt') as reader:
        for line in reader:
            buffer.append(line)

            if 'err' in line.lower():
                # add n/2 more lines to buffer, so err line is approx in middle.
                # similar to doing grep -A 50 -B 50 err
                for i in range(num_lines_per_error // 2):
                    buffer.append(reader.readline())

                outdata.extend([sep] + list(buffer) + [sep])

    if len(outdata) == 0:
        outdata.extend([sep] + list(buffer))

    return ''.join(outdata)


def get_stderr(execution_dir):
    stderr_file = os.path.join(execution_dir, 'stderr')
    stderr_bg_file = os.path.join(execution_dir, 'stderr.background')

    if os.path.exists(stderr_file):
        return extract_err_from_file(stderr_file)
    elif os.path.exists(stderr_bg_file):
        return extract_err_from_file(stderr_file)
    else:
        return 'unable to find error files in {}\n'.format(execution_dir)


class RunFailed(Exception):
    pass


def get_execution_dirs(run_dir):
    outdirs = set()
    for (root, dirs, files) in os.walk(run_dir, topdown=True):
        for val in dirs:
            if val == 'execution':
                outdirs.add(root)
                break
    return sorted(outdirs)


def get_rc_code(execution_dir):
    rcfile = os.path.join(execution_dir, 'rc')
    with open(rcfile, 'rt') as reader:
        data = reader.readlines()
        assert len(data) == 1
        data = data[0].strip()

        return data


def check_for_success(execution_dir, successful_return_codes=('0',)):
    attempt_1 = os.path.join(execution_dir, 'execution')
    rc_code_attempt_1 = get_rc_code(attempt_1)

    if rc_code_attempt_1 in successful_return_codes:
        return True

    retries = [os.path.join(execution_dir,  v, 'execution') for v in os.listdir(execution_dir) if v.startswith('attempt-')]
    retry_rc = [get_rc_code(v) for v in retries]

    for val in retry_rc:
        if val in successful_return_codes:
            return True

    return False


def get_error(execution_dir, successful_return_codes=('0',)):
    attempt_1 = os.path.join(execution_dir, 'execution')
    rc_code_attempt_1 = get_rc_code(attempt_1)

    data = {1: (attempt_1, rc_code_attempt_1)}

    for v in os.listdir(execution_dir):
        if not v.startswith('attempt-'):
            continue

        attempt_num = int(v[len('attempt-'):])
        attempt_dir = os.path.join(execution_dir, v, 'execution')
        attempt_rc = get_rc_code(attempt_dir)

        data[attempt_num] = (attempt_dir, attempt_rc)

    all_rc_codes = [v[1] for k, v in data.items()]
    assert len(set(all_rc_codes).intersection(set(successful_return_codes))) == 0

    last_attempt = sorted(data.keys())[-1]
    last_attempt_dir, last_attempt_rc = data[last_attempt]

    error = get_stderr(last_attempt_dir)
    return error


def debug(cromwell_execution_dir, wf_name, run_id, successful_return_codes=('0',)):
    logging.getLogger('mondrian_runner.debug').warning(
        'detected failed status, extracting errors ...'
    )

    run_dir = os.path.join(cromwell_execution_dir, wf_name, run_id)
    assert os.path.exists(run_dir)

    all_errors = []
    for execution_dir in get_execution_dirs(run_dir):

        if check_for_success(execution_dir, successful_return_codes=successful_return_codes) is False:
            error = get_error(execution_dir, successful_return_codes=successful_return_codes)
            all_errors.append(error)

    raise RunFailed(''.join(all_errors))
