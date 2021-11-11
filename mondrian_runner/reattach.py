from mondrian_runner import utils
import os

def reattach(server_url, outdir, run_id, workflow_log_dir):
    if not run_id:
        run_id = utils.get_id_from_tempdir(outdir)

    logfile = os.path.join(workflow_log_dir, 'workflow.{}.log'.format(run_id))

    status = utils.wait(server_url, run_id, logfile)

    if not status == 'succeeded':
        raise Exception('pipeline fail, status: {}'.format(status))
