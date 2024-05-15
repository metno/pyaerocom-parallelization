#!/usr/bin/env python3
"""
small helper program to read a aeroval config from a json file

"""
import argparse

import simplejson as json
import jsonpickle
from fnmatch import fnmatch
from aeroval_parallelize.const import JSON_EXT, PICKLE_JSON_EXT


def main():
    parser = argparse.ArgumentParser(
        description="small helper to run aeroval configs from json files"
    )
    parser.add_argument("files", help="file(s) to read", nargs="+")
    parser.add_argument(
        "-d", "--dryrun", help="dry run, just print the config", action="store_true"
    )

    args = parser.parse_args()
    options = {}
    if args.files:
        options["files"] = args.files
        # to avoid that lustre access is checked if the help just needs to be printed
        from pyaerocom.aeroval import EvalSetup, ExperimentProcessor

    if args.dryrun:
        options["dryrun"] = True
    else:
        options["dryrun"] = False

    for _file in options["files"]:
        if fnmatch(_file, f"*{JSON_EXT}"):
            with open(_file, "r", encoding="utf-8") as infile:
                CFG = json.load(infile)
        elif fnmatch(_file, f"*{PICKLE_JSON_EXT}"):
            with open(_file, "r") as infile:
                json_string = infile.read()
            CFG = jsonpickle.decode(json_string)
        else:
            print(f"skipping file {_file} due to wrong file extension")
            continue

        stp = EvalSetup(
            **CFG,
        )
        ana = ExperimentProcessor(stp)
        if not options["dryrun"]:
            res = ana.run()
        else:
            print(stp)


if __name__ == "__main__":
    main()
