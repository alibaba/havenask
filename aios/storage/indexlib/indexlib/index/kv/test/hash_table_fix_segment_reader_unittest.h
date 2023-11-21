#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/kv/hash_table_fix_segment_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class HashTableFixSegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    HashTableFixSegmentReaderTest();
    ~HashTableFixSegmentReaderTest();

    DECLARE_CLASS_NAME(HashTableFixSegmentReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestValueWithTs();
    void TestValueWithoutTs();

private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mDocStr;
    config::KVIndexConfigPtr mKVConfig;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HashTableFixSegmentReaderTest, TestValueWithTs);
INDEXLIB_UNIT_TEST_CASE(HashTableFixSegmentReaderTest, TestValueWithoutTs);
}} // namespace indexlib::index
