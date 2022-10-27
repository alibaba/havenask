#/bin/bash

#sh addr2line_stack_tracer.sh [$1=StackTracerLogString]
#$1 [format]: executable_file_path(demangle string):[symbol string]:mapping address:offset

#input="/ssd/pujian/develop/autil/build/debug64/lib/libautil.so.12(autil::StackTracer::getTraceId() const):[_ZNK5autil11StackTracer10getTraceIdEv]0x7f4504b1dbed:0xa7"
input=$1

# get exec file: /ssd/pujian/develop/autil/build/debug64/lib/libautil.so.12
targetExecFile=`echo $input | awk -F '(' '{print $1}'`
pos=${#targetExecFile}

# get demangel string: autil::StackTracer::getTraceId() const
tmpstr=`echo ${input:$pos} | awk -F '[' '{print $1}'`
demangelStr=${tmpstr:1:${#tmpstr}-3}

# get symbol string: _ZNK5autil11StackTracer10getTraceIdEv
symbolStr=`echo $input | awk -F '[' '{print $2}' | awk -F ']' '{print $1}'`

# get map address: 0x7f4504b1dbed
address=`echo $input | awk -F '[' '{print $2}' | awk -F ']' '{print $2}' | awk -F ':' '{print $1}'`

# get offset: 0xa7
offset=`echo $input | awk -F '[' '{print $2}' | awk -F ']' '{print $2}' | awk -F ':' '{print $2}'`

echo $demangelStr

echo "########################################################################################"
echo "target exec file : $targetExecFile"
echo "demangel name : $demangelStr"
echo "symbol name : $symbolStr"
echo "mapping address : $address"
echo "offset : $offset"
echo "########################################################################################"
echo "1. addr2line : map address [$address]"
addr2line -Cif -e $targetExecFile $address

echo "########################################################################################"
addressAddOffset=`printf '0x%x\n' $(( $address + $offset ))`
echo "2. addr2line : (map address + offset) [$addressAddOffset]"
addr2line -Cif -e $targetExecFile $addressAddOffset

echo "########################################################################################"
objAddr="0x"`nm -Dlan $targetExecFile | grep $symbolStr | awk '{print $1}'`
objAddrAddOffset=`printf '0x%x\n' $(( $objAddr + $offset ))`
echo "3. addr2line : (nm address + offset) [$objAddrAddOffset]" 
addr2line -Cif -e $targetExecFile $objAddrAddOffset


:<<!
1. 如果关注的一行backtrace位于一个可执行文件中，那么直接addr2line -e <executable> <address>

2. 如果关注的backtrace位于一个动态链接库中，那么麻烦一些，因为动态链接库的基地址不是固定的。
这个时候，首先要把进程的memory map找来。在Linux下，进程的memory map可以在/proc/<pid>/maps文件中得到(或者pmap -p <pid>获得)然后在这个文件中找到动态链接库的基地址，然后将backtrace中的地址 - 动态链接库的基地址，得到偏移地址offset address, 最后addr2line -e <shared library> <offset address>。

3. 如果知道堆栈的符号信息，可以通过nm XXX.so | grep symbol找到符号对应地址，然后添加堆栈上的偏移，再通过addr2line -e <shared library> <cal address>来获取，本脚本采用第三种方法配合backtrace功能使用
!
