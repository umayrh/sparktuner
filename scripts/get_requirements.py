"""
Simple module for converting a requirements.txt file into
a list of dependencies used as input to pivy-importer.
Assumes that requirements.txt is present in the directory
that this script in invoked from.
Requires that requirements.txt only contain lines with
either package name or both package name and version. If
both are specified, that it requires that the version be
either exact ("==") or minimum (">="), treating them both
as the same.

Example usages:

$ python ../etc/get_requirements.py

pypi:opentuner:0.8.0 pypi:humanfriendly:4.17 pypi:chainmap:1.0.2
"""
import os
import sys
import subprocess
import re
import requests
from pkg_resources import parse_version


def get_latest_version(name):
    print("workin on " + name)
    url = "https://pypi.python.org/pypi/{}/json".format(name)
    return sorted(requests.get(url).json()["releases"], key=parse_version)[-1]


def get_dependencies():
    dependencies = []
    with open("requirements.txt") as fp:
        for line in fp:
            tokens = re.split('==|>=', line.strip())
            token_len = len(tokens)
            if token_len == 1:
                name = tokens[0]
                version = get_latest_version(name)
            elif token_len == 2:
                name = tokens[0]
                version = tokens[1]
            else:
                raise ValueError("Bad requirements.txt")
            dependencies.append(":".join([name, version]))
    return dependencies


if __name__ == '__main__':
    pivyRepo = sys.argv[1] if len(sys.argv) > 1 else None
    debug = str(sys.argv[2]).lower() == "true" if len(sys.argv) > 2 else False
    outputSep = sys.argv[3] if len(sys.argv) > 3 else " "

    dependency_list = get_dependencies()
    prefixed_dep = map(lambda p: "pypi:" + p, dependency_list)
    print(outputSep.join(prefixed_dep))

    if pivyRepo is not None:
        dir_path = os.path.dirname(os.path.abspath(__file__))
        jar_file = os.path.join(dir_path, "pivy-importer-0.9.9-all.jar")
        cmd = ['java', '-jar', jar_file, '--repo', pivyRepo]
        if debug:
            cmd.extend(['--debug'])
        cmd.extend(dependency_list)
        print(subprocess.check_output(cmd, stderr=subprocess.STDOUT))
