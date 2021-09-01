from mondrian_runner.abort import abort
from mondrian_runner.cli import parse_args
from mondrian_runner.reattach import reattach
from mondrian_runner.run import runner
from mondrian_runner import utils
import os

def main():
    args = parse_args()

    utils.init_logger(os.path.join(args['outdir']), args['log_level'])


    if args["which"] == "run":
        runner(args['server_url'], args['pipeline_name'], args['input_json'], args['options_json'], args['outdir'])
        return

    if args["which"] == "reattach":
        reattach(args['server_url'], args['outdir'], args['run_id'])
        return

    if args["which"] == "abort":
        abort(args['server_url'], args['outdir'], args['run_id'])
        return
