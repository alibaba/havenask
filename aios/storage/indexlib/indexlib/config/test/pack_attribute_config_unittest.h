#pragma once

#include "autil/Log.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

class PackAttributeConfigTest : public INDEXLIB_TESTBASE
{
public:
    PackAttributeConfigTest();
    ~PackAttributeConfigTest();

public:
    void TestJsonize();
    void TestCreateAttributeConfig();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackAttributeConfigTest, TestCreateAttributeConfig);
}} // namespace indexlib::config
