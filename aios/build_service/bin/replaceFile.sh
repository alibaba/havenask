#!/bin/sh

if [ $# -lt 1 ]; then
    echo "USAGE: $0 fileName"
    exit -1
fi;

sed -i "s/ISEARCH/ISEARCH_BS/g" $1
sed -i "s/ha3\//build_service\//g" $1
sed -i "s/HA3/BS/g" $1
sed -i "/isearch.h/d" $1

#find $1 -name *.cpp -or -name *.h -or -name SConscript | xargs -I {} sed -i "s/$2/$3/g" {}
#find ./ -name *.pl -or -name *.py | xargs -I {} sed -i 's/ha2/ha3/g' {}
#find ./ -name *.conf -or -name *.json | xargs -I {} sed -i 's/ha2/ha3/g' {}
#find $1 -name *.yy -or -name *.ll | xargs -I {} sed -i "s/$2/$3/g" {}
#find $1 -name *.yy -or -name *.ll | xargs -I {} sed -i "s/^$2/$3/g" {}
#find ./ -name *.ed -or -name *.in | xargs -I {} sed -i 's/ha2/ha3/g' {}
#find ./ -name SConstruct | xargs -I {} sed -i 's/ha2/ha3/g' {}