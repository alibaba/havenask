#!/bin/bash
#set -x
set -e

DIR="$(dirname "${BASH_SOURCE[0]}")"
DIR="$(realpath "${DIR}")"
cd ${DIR}

# build
bazel build //package/havenask:havenask_package_tar --config=havenask 2>&1 | tee build.log

# install
INSTALL_DIR=ha3_install
rm -rf ${INSTALL_DIR}
mkdir ${INSTALL_DIR}
tar xvf bazel-bin/package/havenask/havenask_package_tar.tar -C ${INSTALL_DIR}


echo "build & install finished: [${DIR}/${INSTALL_DIR}]"
