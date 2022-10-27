#!/bin/bash
set +x
python ./bin/replace_one_file.py $1

sed -i "s/CPPUNIT_ASSERT_EQUAL/ASSERT_EQ/g" $1
sed -i "s/CPPUNIT_ASSERT/ASSERT_TRUE/g" $1
sed -i "/CPPUNIT_TEST_SUITE/d" $1
sed -i "/CPPUNIT_TEST/d" $1
sed -i "/CPPUNIT_TEST_SUITE_END/d" $1
sed -i "/HelperMacros.h/d" $1
sed -i "1 i #include<unittest/unittest.h>" $1
sed -i "s/CppUnit::TestFixture/TESTBASE/g" $1
sed -i "/cppunit/d" $1

echo $1
case_name=`echo $1 | awk -F'.' '{print $1}'`
rm $case_name".h"
