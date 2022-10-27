#!/bin/bash

while read line; do
    case_name=`echo $line | awk -F'.' '{print $1}'`
    header_content=`cat $case_name".h"`
    class_declare=`cat $case_name".h" | awk -F'(' 'BEGIN{echo=0} { if ($1=="END_BS_NAMESPACE"){echo=0;};if (echo==1&&index($0,"void test")==0) {print $0;}if ($1=="BEGIN_BS_NAMESPACE"){echo=1};}'`
    # cat $line | while RS=# read other_line;do
    #     if [[ $other_line == *"BS_LOG_SETUP"* ]];then
    #         cat $case_name".h" | awk -F'(' 'BEGIN{echo=0} { if ($1=="END_BS_NAMESPACE"){echo=0;};if (echo==1&&index($0,"void test")==0) {print $0;}if ($1=="BEGIN_BS_NAMESPACE"){echo=1};}' | while RS=# read header_line;do
    #         echo $header_line
    #         done
    #     fi
    #     echo $other_line
    # done
    
    # echo $content
    #cat $line | awk  -F'(' "{if (\$1==\"BS_LOG_SETUP\"){print \$0;print "$class_declare"}else{print \$0}}"
    # $new_content=${`cat $line`/$case_name::$case_name/$class_declare"\n"$case_name::$case_name/}
    # sed_cmd="s/"$case_name"::"$case_name"/"$class_declare$case_name"::"$case_name"/"
    # cat $line | sed '$sed_cmd'
    # sed 's/void $case_name::test\(.*\)()/TEST_F($case_name, test\1)/g'
    
    python ../../../bin/replace_one_file.py $case_name
    rm $case_name".h"
done