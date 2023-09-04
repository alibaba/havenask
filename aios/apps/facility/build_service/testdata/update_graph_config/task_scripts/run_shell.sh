#!/bin/sh

# test arguments
echo "test arguments: $*"

index_path=$1
cp_path=$index_path/shell_index

bin=fs_util
$bin rm $cp_path
dirs=`$bin ls $index_path`

$bin mkdir $cp_path
for i in $dirs
do
    $bin cp -r $index_path/$i $cp_path
done
