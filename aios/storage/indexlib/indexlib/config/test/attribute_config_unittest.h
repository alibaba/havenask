#ifndef __INDEXLIB_ATTRIBUTECONFIGTEST_H
#define __INDEXLIB_ATTRIBUTECONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {

class AttributeConfigTest : public INDEXLIB_TESTBASE
{
public:
    AttributeConfigTest();
    ~AttributeConfigTest();

public:
    void TestCreateAttributeConfig();
    void TestAssertEqual();
    void TestUpdatable();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeConfigTest, TestCreateAttributeConfig);
INDEXLIB_UNIT_TEST_CASE(AttributeConfigTest, TestAssertEqual);
INDEXLIB_UNIT_TEST_CASE(AttributeConfigTest, TestUpdatable);
}} // namespace indexlib::config

#endif //__INDEXLIB_ATTRIBUTECONFIGTEST_H
