#pragma once

#include "autil/Log.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/util/testutil/unittest.h"

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
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeConfigTest, TestCreateAttributeConfig);
INDEXLIB_UNIT_TEST_CASE(AttributeConfigTest, TestAssertEqual);
INDEXLIB_UNIT_TEST_CASE(AttributeConfigTest, TestUpdatable);
}} // namespace indexlib::config
