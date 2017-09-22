#!/usr/bin/env python3
"""
Rename hash or dict logs when adding new parameters to a task.

Old log names are found by proving the <param>s to the tasks `omit` kwarg, then generating the list of output.

Usage:
    rename-logs.py <meta> <task> <param>... [--meta-params <prms>] [--overwrite]
    rename-logs.py (-h | --help)

Options:
    -h --help               Show this screen.
    --meta-params <prms>    Parameters to set up the meta task.
    --overwrite             When moving, overwrite any existing logs. Use if you (mistakenly) began re-running before renaming logs.
"""
from src import *
import luigi
from luigi.task import flatten
from docopt import docopt
import json

if __name__ == "__main__":
    args = docopt(__doc__)

    meta = globals()[args["<meta>"]]
    task = globals()[args["<task>"]]

    inst = meta(**(json.loads(args["--meta-params"]) if args["--meta-params"] is not None else {}))
    tasks = [t for t in flatten(inst.requires())
             if isinstance(t, task)]
    print(len(tasks))

    for t in tasks:
        old_target = t.output(omit=args["<param>"])
        if old_target.exists():
            print("Renaming {} → {}".format(old_target.path, t.output().path))
            old_target.move(t.output().path, raise_if_exists=not args["--overwrite"])
        else:
            print("Old log {} does not exist. Skipping.".format(old_target.path))
