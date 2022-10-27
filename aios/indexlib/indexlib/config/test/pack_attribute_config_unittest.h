#ifndef __INDEXLIB_PACKATTRIBUTECONFIGTEST_H
#define __INDEXLIB_PACKATTRIBUTECONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/configurator_define.h"

IE_NAMESPACE_BEGIN(config);

class PackAttributeConfigTest : public INDEXLIB_TESTBASE {
public:
    PackAttributeConfigTest();
    ~PackAttributeConfigTest();
public:
    void SetUp();
    void TearDown();
    void TestJsonize();
    void TestCreateAttributeConfig();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackAttributeConfigTest, TestCreateAttributeConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PACKATTRIBUTECONFIGTEST_H
