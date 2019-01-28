# Spark Tuner

[![Build Status](https://travis-ci.org/umayrh/sparktuner.svg?branch=master)](https://travis-ci.org/umayrh/sparktuner)
[![Coverage Status](https://coveralls.io/repos/github/umayrh/sparktuner/badge.svg?branch=master)](https://coveralls.io/github/umayrh/sparktuner?branch=master)
[![Maintainability](https://api.codeclimate.com/v1/badges/1b9ae406a6e8b922405a/maintainability)](https://codeclimate.com/github/umayrh/sparktuner/maintainability)

## Setup, build, and usage

#### Setup

This package assumes that Apache Spark is installed, and the following environment
variables have already been set: `SPARK_HOME`, and, optionally, `HADOOP_CONF_DIR`. 
See [dev-README.md](./dev-README.md) for details.

All Python dependencies are listed in:
* `requirements.txt`
* `build.gradle`
* `setup.py`

#### Build

* `./gradlew clean build` to download and install all dependencies from scratch, and run tests.
* `./gradlew flake8` to lint for style issues.
* `./gradlew pytest` to run tests.
* `./gradlew build -x getRequirements` to install all dependencies (assumes they've already
been downloaded).

Some interesting build artifacts are:
* `build/deployable/bin/sparktuner`
* `build/deployable/bin/sparktuner.pex`
* `build/distributions/sparktuner-0.1.0.tar.gz`
* `build/wheel-cache/sparktuner-0.1.0-py2-none-any.whl`

The Python virtual environment resides in `build/venv`, and can be activated using
`source build/venv/bin/activate` and deactivated using `deactivate`.

#### Usage

To see usage information, `./build/deployable/bin/sparktuner --help`

Sample commands:
* `build/deployable/bin/sparktuner --no-dups --name sartre_spark_sortre --path ../../sparkScala/sort/build/libs/sort-0.1-all.jar --deploy_mode client --master "local[*]" --class com.umayrh.sort.Main --spark_parallelism "1,10" --program_conf "10000 /tmp/sparktuner_sort"`

* `build/deployable/bin/sparktuner --no-dups --name sartre_spark_sortre --path ../../sparkScala/sort/build/libs/sort-0.1-all.jar --deploy_mode client --master "local[*]" --class com.umayrh.sort.Main --executor_memory "50mb,1gb" --program_conf "10000 /tmp/sparktuner_sort"`

* `build/deployable/bin/sparktuner --no-dups --name sartre_spark_sortre --path ../../sparkScala/sort/build/libs/sort-0.1-all.jar --deploy_mode client --master "local[*]" --class com.umayrh.sort.Main --driver_memory "1GB,6GB" --program_conf "1000000 /tmp/sparktuner_sort"`
