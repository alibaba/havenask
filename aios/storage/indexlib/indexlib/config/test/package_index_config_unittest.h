#pragma once

#include "autil/Log.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

class PackageIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    PackageIndexConfigTest();
    ~PackageIndexConfigTest();

public:
    void TestJsonize();
    void TestSupportNull();
    void TestCheck();

private:
    config::FieldConfigPtr MakeFieldConfig(const std::string& fieldName, FieldType fieldType, fieldid_t fieldId,
                                           bool isNull);

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackageIndexConfigTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(PackageIndexConfigTest, TestSupportNull);
INDEXLIB_UNIT_TEST_CASE(PackageIndexConfigTest, TestCheck);
}} // namespace indexlib::config
