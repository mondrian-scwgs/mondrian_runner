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
    run = add_global_args(run)
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
        required=True,
        help='server url'
    )
    run.add_argument(
        "--workflow_log_dir",
        required=True,
        help='server url'
    )


    reattach = subparsers.add_parser("reattach")
    reattach.set_defaults(which='reattach')
    reattach = add_global_args(reattach)
    reattach.add_argument(
        "--run_id",
        help='server url'
    )

    abort = subparsers.add_parser("abort")
    abort.set_defaults(which='abort')
    abort = add_global_args(abort)
    abort.add_argument(
        "--run_id",
        help='server url'
    )

    args = vars(parser.parse_args())

    return args
