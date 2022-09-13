import logging
import os

import mondrian_runner.utils as utils
from mondrian_runner.debug import debug


def write_delete_wdl(wdl_path, dirs_to_delete):
    dirs_to_delete = ' '.join(dirs_to_delete)
    wdl_file_string = 'task remove_intermediate_dir{\ncommand{\n'
    wdl_file_string += 'rm -rf {} \n'.format(dirs_to_delete) + '}\n'
    wdl_file_string += 'runtime { \n memory: "4G" \n cpu: 1 \n walltime: "24:00"\n}\n}\n'
    wdl_file_string += 'workflow remove_workflow{\ncall remove_intermediate_dir\n}'

    with open(wdl_path, 'wt') as writer:
        writer.write(wdl_file_string)


def delete_intermediates_workflow(
        server_url, cache_dir, delete_cache_dir, wf_name, workflow_log_dir, execution_dir
):
    run_ids = utils.get_all_ids_from_cache_dir(cache_dir)
    run_dirs = [os.path.join(execution_dir, wf_name, run_id) for run_id in run_ids]

    wdl_file = os.path.join(delete_cache_dir, 'delete_intermediates.wdl')
    write_delete_wdl(wdl_file, run_dirs)

    run_id = utils.submit_pipeline(server_url, wdl_file)

    utils.cache_run_id(run_id, delete_cache_dir)

    logfile = os.path.join(workflow_log_dir, 'workflow.{}.log'.format(run_id))
    status = utils.wait(server_url, run_id, logfile)

    logging.getLogger('mondrian_runner.cleanup').warning(
        'cleanup status: {}'.format(status)
    )


def runner(
        server_url, pipeline_wdl, input_json, options_json,
        cache_dir, mondrian_dir, imports=None, delete_intermediates=False,
        try_reattach=None
):
    with utils.PipelineLock(cache_dir):

        wf_name = utils.get_wf_name_from_input_json(input_json)

        execution_dir = os.path.join(mondrian_dir, 'cromwell-executions')
        workflow_log_dir = os.path.join(mondrian_dir, 'cromwell-workflow-logs')

        utils.makedirs(cache_dir)

        run_id = utils.get_latest_id_from_cache_dir(cache_dir)

        status = None
        if run_id is not None:
            status = utils.check_status(server_url, run_id)

        if run_id is None or try_reattach is False or status not in ['running', 'submitted', 'succeeded']:
            run_id = utils.submit_pipeline(
                server_url, pipeline_wdl, input_json=input_json, options_json=options_json,
                imports=imports
            )
            utils.cache_run_id(run_id, cache_dir)

        status = utils.wait(server_url, run_id, workflow_log_dir)

        if status == 'succeeded':
            if delete_intermediates:
                delete_cache_dir = os.path.join(cache_dir, 'remove_intermediates')
                utils.makedirs(delete_cache_dir)
                delete_intermediates_workflow(
                    server_url, cache_dir, delete_cache_dir, wf_name, workflow_log_dir, execution_dir
                )
            logging.getLogger('mondrian_runner.poll').info('Successfully completed')
        else:
            debug(execution_dir, wf_name, run_id)
