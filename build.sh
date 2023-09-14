#!/bin/bash
#set -x
set -e

DIR="$(dirname "${BASH_SOURCE[0]}")"
DIR="$(realpath "${DIR}")"
cd ${DIR}

OPTIONS="--config=havenask" # default: fastbuild

if test -z $1; then
    MODE='fastbuild'
else
    MODE=$1
fi

if test $MODE = "fastbuild"; then
    OPTIONS="$OPTIONS"
elif test $MODE = "opt"; then
    OPTIONS="$OPTIONS -c opt --copt=-g --strip=never"
elif test $MODE = "opt-strip"; then
    OPTIONS="$OPTIONS -c opt --strip=always"
elif test $MODE = "dbg"; then
    OPTIONS="$OPTIONS -c dbg --strip=never"
elif test $MODE = "asan"; then
    OPTIONS="$OPTIONS -c dbg --strip=never --config=asan"
else
    echo "error: invalid mode, exit!"
    echo "[usage]"
    echo "$0"
    echo "$0 fastbuild"
    echo "$0 dbg"
    echo "$0 opt"
    echo "$0 opt-strip"
    echo "$0 asan"
    exit -1
fi

# build
echo "build mode [$MODE], build option [$OPTIONS]"
bazel build //package/havenask/hape:hape_tar $OPTIONS 2>&1 | tee build.log

# install
INSTALL_DIR=ha3_install
rm -rf ${INSTALL_DIR}
mkdir ${INSTALL_DIR}
tar xvf bazel-bin/package/havenask/hape/hape_tar.tar -C ${INSTALL_DIR}
echo "build & install finished: [${DIR}/${INSTALL_DIR}]"
