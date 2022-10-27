#ifndef __INDEXLIB_INDEXFORMATOPTIONTEST_H
#define __INDEXLIB_INDEXFORMATOPTIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/config/index_schema.h"

IE_NAMESPACE_BEGIN(index);

class IndexFormatOptionTest : public INDEXLIB_TESTBASE {
public:
    IndexFormatOptionTest();
    ~IndexFormatOptionTest();
public:
    void SetUp();
    void TearDown();
    void TestStringIndexForDefaultValue();
    void TestTextIndexForDefaultValue();
    void TestPackIndexForDefaultValue();
    void TestCopyConstructor();
    void TestTfBitmap();
    void TestToStringFromString();
    void TestDateIndexConfig();

private:
    IndexFormatOption GetIndexFormatOption(const std::string& indexName);
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

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEXFORMATOPTIONTEST_H
