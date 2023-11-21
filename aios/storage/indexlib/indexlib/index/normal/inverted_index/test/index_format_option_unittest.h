#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib::index {

class IndexFormatOptionTest : public INDEXLIB_TESTBASE
{
public:
    IndexFormatOptionTest();
    ~IndexFormatOptionTest();

public:
    void CaseSetUp();
    void CaseTearDown();
    void TestStringIndexForDefaultValue();
    void TestTextIndexForDefaultValue();
    void TestPackIndexForDefaultValue();
    void TestCopyConstructor();
    void TestTfBitmap();
    void TestToStringFromString();
    void TestDateIndexConfig();

private:
    LegacyIndexFormatOption GetIndexFormatOption(const std::string& indexName);

private:
    config::IndexSchemaPtr mIndexSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexFormatOptionTest, TestStringIndexForDefaultValue);
INDEXLIB_UNIT_TEST_CASE(IndexFormatOptionTest, TestTextIndexForDefaultValue);
INDEXLIB_UNIT_TEST_CASE(IndexFormatOptionTest, TestPackIndexForDefaultValue);
INDEXLIB_UNIT_TEST_CASE(IndexFormatOptionTest, TestCopyConstructor);
INDEXLIB_UNIT_TEST_CASE(IndexFormatOptionTest, TestTfBitmap);
INDEXLIB_UNIT_TEST_CASE(IndexFormatOptionTest, TestToStringFromString);
INDEXLIB_UNIT_TEST_CASE(IndexFormatOptionTest, TestDateIndexConfig);

} // namespace indexlib::index
