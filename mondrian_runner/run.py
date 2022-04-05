import logging
import os

import mondrian_runner.utils as utils
from mondrian_runner.debug import debug

def submit_pipeline(server_url, wdl_file, input_json, options_json, imports=None):
    logger = logging.getLogger('mondrian_runner.submit')

    cmd = [
        'curl',
        '-X', 'POST',
        '--header', 'Accept: application/json',
        '-v', '{}/api/workflows/v1'.format(server_url),
        '-F', 'workflowSource=@{}'.format(wdl_file),
        '-F', 'workflowInputs=@{}'.format(input_json),
        '-F', 'workflowOptions=@{}'.format(options_json),
    ]

    if imports is not None:
        cmd += ['-F', 'workflowDependencies=@{}'.format(imports)]

    logger.info('running: {}'.format(' '.join(cmd)))

    cmdout, cmderr = utils.run_cmd(cmd)

    run_id = utils.get_run_id(cmdout)

    logger.info("run_id: {}".format(run_id))

    return run_id


def runner(
        server_url, pipeline_wdl, input_json, options_json,
        outdir, mondrian_dir,  imports=None
):
    workflow_log_dir = os.path.join(mondrian_dir, 'cromwell-workflow-logs')

    run_id = submit_pipeline(server_url, pipeline_wdl, input_json, options_json, imports=imports)

    utils.cache_run_id(run_id, outdir)

    logfile = os.path.join(workflow_log_dir, 'workflow.{}.log'.format(run_id))

    status = utils.wait(server_url, run_id, logfile)

    if not status == 'succeeded':
        execution_dir = os.path.join(mondrian_dir, 'cromwell-executions')
        wf_name = utils.get_wf_name(execution_dir, run_id)
        print('detected {} status, extracting errors ...' .format(status))
        debug(execution_dir, wf_name, run_id)

