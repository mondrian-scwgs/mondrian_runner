import logging
import os
import shutil

import mondrian_runner.utils as utils


def submit_pipeline(server_url, wdl_file, input_json, options_json, imports):
    logger = logging.getLogger('mondrian_runner.submit')

    cmd = [
        'curl',
        '-X', 'POST',
        '--header', 'Accept: application/json',
        '-v', '{}/api/workflows/v1'.format(server_url),
        '-F', 'workflowSource=@{}'.format(wdl_file),
        '-F', 'workflowInputs=@{}'.format(input_json),
        '-F', 'workflowOptions=@{}'.format(options_json),
        '-F', 'workflowDependencies=@{}'.format(imports)
    ]

    logger.info('running: {}'.format(' '.join(cmd)))

    cmdout, cmderr = utils.run_cmd(cmd)

    run_id = utils.get_run_id(cmdout)

    logger.info("run_id: {}".format(run_id))

    return run_id


def runner(server_url, pipeline_name, input_json, options_json, outdir, version, workflow_log_dir):
    run_id = submit_pipeline(server_url, pipeline_name, input_json, options_json, version)

    utils.cache_run_id(run_id, outdir)

    logfile = os.path.join(workflow_log_dir, 'workflow.{}.log'.format(run_id))

    status = utils.wait(server_url, run_id, logfile)

    if not status == 'succeeded':
        raise Exception('pipeline fail, status: {}'.format(status))

    options_data = utils.load_options_json(options_json)

    shutil.copyfile(input_json, options_data['out_dir'])
