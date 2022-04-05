import os

from mondrian_runner.debug import debug
from mondrian_runner import utils


def reattach(
        server_url, outdir, run_id, mondrian_dir
):
    if not run_id:
        run_id = utils.get_id_from_tempdir(outdir)

    workflow_log_dir = os.path.join(mondrian_dir, 'cromwell-workflow-logs')

    logfile = os.path.join(workflow_log_dir, 'workflow.{}.log'.format(run_id))

    status = utils.wait(server_url, run_id, logfile)

    if not status == 'succeeded':
        execution_dir = os.path.join(mondrian_dir, 'cromwell-executions')
        wf_name = utils.get_wf_name(execution_dir, run_id)
        print('detected {} status, extracting errors ...' .format(status))
        debug(execution_dir, wf_name, run_id)
