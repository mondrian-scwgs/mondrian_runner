from mondrian_runner import utils


def reattach(server_url, outdir, run_id):
    if not run_id:
        run_id = utils.get_id_from_tempdir(outdir)

    utils.wait(server_url, run_id)
