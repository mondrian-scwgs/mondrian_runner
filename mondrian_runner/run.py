import mondrian_runner.utils as utils


def submit_pipeline(server_url, pipeline_name, input_json, options_json, logger):
    wf_url = utils.get_workflow_url(pipeline_name)

    cmd = ['curl', '-X', 'POST', '--header', 'Accept: application/json', '-v', '{}/api/workflows/v1'.format(server_url),
           '-F', wf_url,
           '-F', 'workflowInputs=@{}'.format(input_json),
           '-F', 'workflowOptions=@{}'.format(options_json)
           ]

    logger.info('running: {}'.format(' '.join(cmd)))

    cmdout, cmderr = utils.run_cmd(cmd)

    run_id = utils.get_run_id(cmdout)

    logger.info("run_id: {}".format(run_id))

    return run_id


def runner(server_url, pipeline_name, input_json, options_json, outdir):
    logger = utils.init_logger(outdir)

    run_id = submit_pipeline(server_url, pipeline_name, input_json, options_json, logger)

    utils.cache_run_id(run_id, outdir)

    utils.wait(server_url, run_id, logger)