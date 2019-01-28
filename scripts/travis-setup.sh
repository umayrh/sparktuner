#!/bin/bash
# This setup script sets up a container-like environment for installing packages
set -ex

OS=$(uname -s)

## Service versions
SPARK_VERSION=${SPARK_VERSION:-"2.4.0"}
HADOOP_VERSION=${HADOOP_VERSION:-"2.7"}

## OS-specific package installation
bootstrap() {
    if [[ "Linux" == "${OS}" ]]; then
        bootstrapLinux
    else
        echo "Unknown OS: ${OS}"
        exit 1
    fi
}

bootstrapLinux() {
    createGlobalEnvFile
    setupSpark
    installPipRequirements
}

## Create a Bash script containing
## 'export' commands
createGlobalEnvFile() {
    echo "#!/bin/bash" > ${GLOBAL_ENV_FILE}
    echo "set -ex" >> ${GLOBAL_ENV_FILE}
}

## Installs a specific version of Spark
setupSpark() {
    local SPARK_DIR_NAME=spark-${SPARK_VERSION}
    SPARK_DIST_NAME=${SPARK_DIR_NAME}-bin-hadoop${HADOOP_VERSION}
    if [[ ! -d "$HOME/.cache/${SPARK_DIST_NAME}" ]]; then
        cd $HOME/.cache
        rm -fr ./${SPARK_DIST_NAME}.tgz*
        # Use axel again when https://github.com/axel-download-accelerator/axel/issues/192
        # has been fixed.
        # axel --quiet http://www-us.apache.org/dist/spark/${SPARK_DIR_NAME}/${SPARK_DIST_NAME}.tgz
        wget --quiet http://www-us.apache.org/dist/spark/${SPARK_DIR_NAME}/${SPARK_DIST_NAME}.tgz
        ls -alh ${SPARK_DIST_NAME}.tgz
        tar -xf ./${SPARK_DIST_NAME}.tgz
        cd ..
    fi
    export SPARK_HOME="${HOME}/.cache/${SPARK_DIST_NAME}"
    # Writing env variables to a file that can be source later by a different
    # process. This seems to be better compared to hard-coding all variables in
    # an "env: global" block in .travis.yaml
    echo "export SPARK_HOME=\"${HOME}/.cache/${SPARK_DIST_NAME}\"" >> ${GLOBAL_ENV_FILE}
    # TODO: need a more systematic method for setting up Spark properties
    echo "spark.yarn.jars=${SPARK_HOME}/jars/*.jar" > ${SPARK_HOME}/conf/spark-defaults.conf
}

## Installs CI-specific Python packages
installPipRequirements() {
    cd ${TRAVIS_BUILD_DIR}
    pip install --user -r test_requirements.txt
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
