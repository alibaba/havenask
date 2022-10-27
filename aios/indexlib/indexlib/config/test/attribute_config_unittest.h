#ifndef __INDEXLIB_ATTRIBUTECONFIGTEST_H
#define __INDEXLIB_ATTRIBUTECONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/attribute_config.h"

IE_NAMESPACE_BEGIN(config);

class AttributeConfigTest : public INDEXLIB_TESTBASE {
public:
    AttributeConfigTest();
    ~AttributeConfigTest();
public:
    void SetUp();
    void TearDown();
    void TestCreateAttributeConfig();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeConfigTest, TestCreateAttributeConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ATTRIBUTECONFIGTEST_H
