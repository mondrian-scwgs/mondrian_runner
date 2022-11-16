import os
import shutil

import mondrian_runner.utils as utils
import pkg_resources


def pull_cromwell_jar(download_dir):
    cmd = ['wget', 'https://github.com/broadinstitute/cromwell/releases/download/84/cromwell-84.jar', '-o',
           os.path.join(download_dir, 'cromwell.jar')]

    utils.run_cmd_interactive(cmd)

    return os.path.join(download_dir, 'cromwell.jar')


def submit_pipeline(
        wdl_file, cromwell_jar,
        input_json, options_json,
        run_config,
        imports=None
):
    cmd = [
        'java',
        f'-Dconfig.file={run_config}',
        '-jar', cromwell_jar,
        'run', wdl_file,
        '-i', input_json,
        '-o', options_json,
    ]
    if imports:
        cmd += ['--imports', imports]

    utils.run_cmd_interactive(cmd)


def generate_run_config(output_dir):
    reference_config = pkg_resources.resource_filename('mondrian_runner', 'data/run.config')

    shutil.copyfile(reference_config, os.path.join(output_dir, 'run.config'))

    return os.path.join(output_dir, 'run.config')


def local_runner(
        wdl_file, input_json, options_json, cache_dir, cromwell_jar=None, imports=None,
):
    with utils.PipelineLock(cache_dir):
        if cromwell_jar is None:
            cromwell_jar = pull_cromwell_jar(cache_dir)

        run_config = generate_run_config(cache_dir)

        submit_pipeline(
            wdl_file, cromwell_jar, input_json, options_json, run_config,
            imports=imports
        )
