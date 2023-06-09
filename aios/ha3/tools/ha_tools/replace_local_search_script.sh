#!/bin/bash
DIR="$(dirname "${BASH_SOURCE[0]}")"
DIR="$(realpath "${DIR}")"
INSTALL_ROOT=`realpath ~/install_root`

set -x
set -e
rm -rf $INSTALL_ROOT/online/usr/local/lib/python/site-packages/ha_tools/local_search_starter.py
ln -s $DIR/local_search_starter.py $INSTALL_ROOT/online/usr/local/lib/python/site-packages/ha_tools/local_search_starter.py

rm -rf $INSTALL_ROOT/online/usr/local/lib/python/site-packages/ha_tools/local_search_stop.py
ln -s $DIR/local_search_stop.py $INSTALL_ROOT/online/usr/local/lib/python/site-packages/ha_tools/local_search_stop.py
