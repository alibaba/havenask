#ifndef __INDEXLIB_PACKAGEINDEXCONFIGTEST_H
#define __INDEXLIB_PACKAGEINDEXCONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

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
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackageIndexConfigTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(PackageIndexConfigTest, TestSupportNull);
INDEXLIB_UNIT_TEST_CASE(PackageIndexConfigTest, TestCheck);
}} // namespace indexlib::config

#endif //__INDEXLIB_PACKAGEINDEXCONFIGTEST_H
