#!/bin/sh

while read line; do
    sed -i "s/ISEARCH/ISEARCH_BS/g" $line
    sed -i "s/ha3\//build_service\//g" $line
    sed -i "s/HA3/BS/g" $line
    sed -i "/isearch.h/d" $line
    sed -i "s/common\.h/namespace\.h/" $line

    # sed -i 's/NAMESPACE(builder)/NAMESPACE(processor)/g' $line
    # sed -i 's/\/builder\//\/processor\//g' $line
done