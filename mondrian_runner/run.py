import logging
import os

import mondrian_runner.metadata as metadata
import mondrian_runner.utils as utils


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
        outdir, workflow_log_dir, imports=None, add_metadata=False
):
    run_id = submit_pipeline(server_url, pipeline_wdl, input_json, options_json, imports=imports)

    utils.cache_run_id(run_id, outdir)

    logfile = os.path.join(workflow_log_dir, 'workflow.{}.log'.format(run_id))

    status = utils.wait(server_url, run_id, logfile)

    if not status == 'succeeded':
        raise Exception('pipeline fail, status: {}'.format(status))

    if add_metadata:
        metadata.add_metadata(options_json, input_json, pipeline_wdl)
