import os

from mondrian_runner import utils
from mondrian_runner.abort import abort
from mondrian_runner.cli import parse_args
from mondrian_runner.reattach import reattach
from mondrian_runner.retry_run import retry_run
from mondrian_runner.generate_bsub_command import generate_bsub_command
from mondrian_runner.run import runner


def main():
    args = parse_args()

    if args["which"] == "retry_run":
        utils.init_console_logger(args['log_level'])
        retry_run(
            args['cwd'],
            args['docker_cwd'],
            args['singularity_img'],
            args['env_setup_command'],
            args['memory_gb'],
            args['walltime'],
            args['cores'],
            args['retries'],
            args['multiplier'],
            args['job_name'],
            args['bind_mounts'],
            args['lsf_extra_args'],
            max_mem=args['max_mem'],
            kill_hung_jobs=args['kill_hung_jobs']
        )
        return
    elif args['which'] == 'generate_bsub_command':
        generate_bsub_command(
            args["cwd"], args["multiplier"], args["walltime"], args["memory_gb"],
            args["cpu"], args["job_name"], args["out"], args["err"], args["docker_cwd"],
            args["singularity_img"], args["job_shell"], args["docker_script"],
            max_mem=args['max_mem']
        )

    utils.makedirs(args['outdir'])

    utils.init_file_logger(os.path.join(args['outdir']), args['log_level'])

    if args["which"] == "run":
        runner(args['server_url'], args['wdl_file'], args['input_json'], args['options_json'], args['outdir'],
               args['mondrian_dir'], imports=args['imports'])
        return

    if args["which"] == "reattach":
        reattach(args['server_url'], args['outdir'], args['run_id'], args['mondrian_dir'])
        return

    if args["which"] == "abort":
        abort(args['server_url'], args['outdir'], args['run_id'])
        return
