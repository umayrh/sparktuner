#!/bin/bash

# This setup script sets up a container-like environment for installing packages

set -ex

OS=$(uname -s)

## Service versions
# TODO: this should really come from sparkScala/gradle.properties
SPARK_VERSION=${SPARK_VERSION:-"2.4.0"}
HADOOP_VERSION=${HADOOP_VERSION:-"2.7"}

## Check OS type
bootstrap() {
    if [[ "Linux" == "${OS}" ]]; then
        bootstrapLinux
    else
        echo "Unknown OS: ${OS}"
        exit 1
    fi
}

bootstrapLinux() {
    setupSpark
}

## Installs a specific version of Spark
setupSpark() {
    local SPARK_DIR_NAME=spark-${SPARK_VERSION}
    if [[ ! -d "$HOME/.cache/${SPARK_DIR_NAME}" ]]; then
        cd $HOME/.cache
        SPARK_DIST_NAME=${SPARK_DIR_NAME}-bin-hadoop${HADOOP_VERSION}
        rm -fr ./${SPARK_DIST_NAME}.tgz*
        axel --quiet http://www-us.apache.org/dist/spark/${SPARK_DIR_NAME}/${SPARK_DIST_NAME}.tgz
        tar -xf ./${SPARK_DIST_NAME}.tgz
        export SPARK_HOME=`pwd`/${SPARK_DIST_NAME}
        # TODO: need a more systematic method for setting up Spark properties
        echo "spark.yarn.jars=${SPARK_HOME}/jars/*.jar" > ${SPARK_HOME}/conf/spark-defaults.conf
        cd ..
    fi
}

# Retry a given command
retry() {
    if "$@"; then
        return 0
    fi
    for wait_time in 5 20 30 60; do
        echo "Command failed, retrying in ${wait_time} ..."
        sleep ${wait_time}
        if "$@"; then
            return 0
        fi
    done
    echo "Failed all retries!"
    exit 1
}

bootstrap
