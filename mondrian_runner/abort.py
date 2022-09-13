from mondrian_runner import utils


def abort(server_url, cache_dir, run_id):
    if cache_dir is None and run_id is None:
        raise Exception('Please specify either cache_dir or run_id')

    if cache_dir is not None and run_id is not None:
        raise Exception('Please specify either cache_dir or run_id, not both')

    if run_id is None:
        run_id = utils.get_latest_id_from_tempdir(cache_dir)

    cmd = ['curl', '-X', 'POST', '{}/api/workflows/v1/{}/abort'.format(server_url, run_id)]

    utils.run_cmd(cmd)
