from mondrian_runner import utils


def abort(server_url, tempdir, run_id):
    if not run_id:
        run_id = utils.get_id_from_tempdir(tempdir)

    cmd = ['curl', '-X', 'POST', '{}/api/workflows/v1/{}/abort'.format(server_url, run_id)]

    utils.run_cmd(cmd)
