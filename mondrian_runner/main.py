from mondrian_runner import utils
from mondrian_runner.abort import abort
from mondrian_runner.check_alive import check_alive
from mondrian_runner.cli import parse_args
from mondrian_runner.generate_bsub_command import generate_bsub_command
from mondrian_runner.run import runner


def main():
    args = parse_args()

    if args['which'] == 'check_alive':
        check_alive(
            args['job_id'], kill_hung_jobs=args['kill_hung_jobs']
        )
    elif args['which'] == 'generate_bsub_command':
        generate_bsub_command(
            args["cwd"], args["multiplier"], args["walltime"], args["memory_gb"],
            args["cpu"], args["job_name"], args["out"], args["err"], args["docker_cwd"],
            args["singularity_img"], args["job_shell"], args["docker_script"],
            max_mem=args['max_mem'], bind_mounts=args['bind_mounts'],
            lsf_extra_args=args['lsf_extra_args']
        )
    elif args["which"] == "run":
        utils.init_console_logger(args['log_level'])
        runner(
            args['server_url'], args['wdl_file'], args['input_json'],
            args['options_json'], args['cache_dir'], args['mondrian_dir'],
            imports=args['imports'],
            delete_intermediates=args['delete_intermediates'],
            try_reattach=args['try_reattach']
        )
    elif args["which"] == "abort":
        utils.init_console_logger(args['log_level'])
        abort(args['server_url'], args['outdir'], args['run_id'])
    else:
        raise Exception('unknown parser option: {} '.format(args['which']))
