import argparse


def add_global_args(parser):
    parser.add_argument(
        "--server_url",
        required=True,
        help='server url'
    )

    parser.add_argument(
        "--outdir",
        required=True,
        help='server url'
    )

    parser.add_argument(
        "--log_level",
        default='INFO',
        help='server url'
    )
    return parser


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    subparsers = parser.add_subparsers()

    run = subparsers.add_parser("run")
    run.set_defaults(which='run')
    run.add_argument(
        "--wdl_file",
        required=True,
        help='server url'
    )
    run.add_argument(
        "--input_json",
        required=True,
        help='server url'
    )
    run.add_argument(
        "--options_json",
        required=True,
        help='server url'
    )
    run.add_argument(
        "--imports",
        help='server url'
    )
    run.add_argument(
        "--server_url",
        required=True,
        help='server url'
    )
    run.add_argument(
        "--cache_dir",
        required=True,
        help='server url'
    )
    run.add_argument(
        "--log_level",
        default='INFO',
        help='server url'
    )
    run.add_argument(
        "--mondrian_dir",
        required=True,
        help='server url'
    )
    run.add_argument(
        "--delete_intermediates",
        action='store_true',
        default=False,
        help='server url'
    )
    run.add_argument(
        "--try_reattach",
        action='store_true',
        default=False,
        help='server url'
    )

    local_run = subparsers.add_parser("local_run")
    local_run.set_defaults(which='local_run')
    local_run.add_argument(
        "--wdl_file",
        required=True,
        help='server url'
    )
    local_run.add_argument(
        "--input_json",
        required=True,
        help='server url'
    )
    local_run.add_argument(
        "--options_json",
        required=True,
        help='server url'
    )
    local_run.add_argument(
        "--imports",
        help='server url'
    )
    local_run.add_argument(
        "--cache_dir",
        required=True,
        help='server url'
    )
    local_run.add_argument(
        "--cromwell_jar",
        help='server url'
    )

    abort = subparsers.add_parser("abort")
    abort.set_defaults(which='abort')
    abort.add_argument(
        "--server_url",
        required=True,
        help='server url'
    )
    abort.add_argument(
        "--run_id",
        help='server url'
    )
    abort.add_argument(
        "--cache_dir",
        help='server url'
    )

    generate_bsub_command = subparsers.add_parser("generate_bsub_command")
    generate_bsub_command.set_defaults(which='generate_bsub_command')
    generate_bsub_command.add_argument(
        "--cwd", required=True
    )
    generate_bsub_command.add_argument(
        "--multiplier", type=int, default=2
    )
    generate_bsub_command.add_argument(
        "--walltime", required=True
    )
    generate_bsub_command.add_argument(
        "--memory_gb", type=int, required=True
    )
    generate_bsub_command.add_argument(
        "--cpu", type=int, required=True
    )
    generate_bsub_command.add_argument(
        "--job_name", required=True
    )
    generate_bsub_command.add_argument(
        "--out", required=True
    )
    generate_bsub_command.add_argument(
        "--err", required=True
    )
    generate_bsub_command.add_argument(
        "--docker_cwd", required=True
    )
    generate_bsub_command.add_argument(
        "--singularity_img", required=True
    )
    generate_bsub_command.add_argument(
        "--job_shell", required=True
    )
    generate_bsub_command.add_argument(
        "--docker_script", required=True
    )
    generate_bsub_command.add_argument(
        "--max_mem",
        default=450,
        type=int
    )
    generate_bsub_command.add_argument(
        "--max_walltime_hrs",
        default=720,
        type=int
    )
    generate_bsub_command.add_argument(
        "--bind_mounts", nargs='*', default=['/data1', '/scratch', '/usersoftware']
    )
    generate_bsub_command.add_argument(
        "--lsf_extra_args",
    )

    check_alive = subparsers.add_parser("check_alive")
    check_alive.set_defaults(which='check_alive')
    check_alive.add_argument(
        "--job_id", required=True
    )
    check_alive.add_argument(
        "--kill_hung_jobs", default=False, action='store_true'
    )

    args = vars(parser.parse_args())

    return args
