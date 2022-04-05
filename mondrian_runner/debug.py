import os
from collections import deque


class RunFailed(Exception):
    pass

def get_rc_code(execution_dir):
    rcfile = os.path.join(execution_dir, 'rc')
    with open(rcfile, 'rt') as reader:
        data = reader.readlines()
        assert len(data) == 1
        data = data[0].strip()

        return data


def get_failed_jobs(run_dir, successful_return_codes=(0,)):
    for (root, dirs, files) in os.walk(run_dir, topdown=True):
        if root.endswith('execution'):
            if get_rc_code(root) not in successful_return_codes:
                yield root


def get_execution_dirs(run_dir):
    for (root, dirs, files) in os.walk(run_dir, topdown=True):
        if root.endswith('execution'):
            yield root


def extract_err_from_file(filepath, max_file_length=300, num_lines_per_error=100):
    sep = '-' * 80 + '\n'

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


def debug(cromwell_execution_dir, wf_name, run_id, successful_return_codes=(0,)):
    run_dir = os.path.join(cromwell_execution_dir, wf_name, run_id)
    assert os.path.exists(run_dir)

    all_errors = []
    for execution_dir in get_execution_dirs(run_dir):
        rc_code = get_rc_code(execution_dir)

        if rc_code not in successful_return_codes:
            error = get_stderr(execution_dir)

            all_errors.append(
                'failed job with code {} found in dir: {}\n'.format(rc_code, execution_dir)
            )
            all_errors.append(error)

    raise RunFailed(''.join(all_errors))
