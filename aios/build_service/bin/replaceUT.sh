#!/bin/sh

while read line; do
    sed -i "s/ISEARCH/ISEARCH_BS/g" $line
    sed -i "s/ha3\//build_service\//g" $line
    sed -i "s/HA3/BS/g" $line
    sed -i "/isearch.h/d" $line
    sed -i "s/CPPUNIT_ASSERT_EQUAL/ASSERT_EQ/g" $line
    sed -i "s/CPPUNIT_ASSERT/ASSERT_TRUE/g" $line
    sed -i "/CPPUNIT_TEST_SUITE/d" $line
    sed -i "/CPPUNIT_TEST/d" $line
    sed -i "/CPPUNIT_TEST_SUITE_END/d" $line
    sed -i "s/cppunit\/extensions\/HelperMacros.h/build_service\/test\/unittest.h/g" $line
    sed -i "/cppunit/d" $line
    sed -i "s/CppUnit::TestFixture/BUILD_SERVICE_TESTBASE/g" $line
    sed -i "s/common\.h/namespace\.h/" $line

    # sed -i 's/NAMESPACE(builder)/NAMESPACE(processor)/g' $line
    # sed -i 's/\/builder\//\/processor\//g' $line
    # sed -i 's/BS_LOG_SETUP(builder/BS_LOG_SETUP(processor/g' $line
done