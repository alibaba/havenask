#ifndef __INDEXLIB_PACKATTRIBUTECONFIGTEST_H
#define __INDEXLIB_PACKATTRIBUTECONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

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
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackAttributeConfigTest, TestCreateAttributeConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_PACKATTRIBUTECONFIGTEST_H
