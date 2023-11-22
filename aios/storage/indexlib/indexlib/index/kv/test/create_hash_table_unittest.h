#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/kv/hash_table_fix_creator.h"
#include "indexlib/index/kv/hash_table_fix_segment_reader.h"
#include "indexlib/index/kv/hash_table_var_creator.h"
#include "indexlib/index/kv/kv_segment_offset_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class CreateHashTableTest : public INDEXLIB_TESTBASE
{
public:
    CreateHashTableTest();
    ~CreateHashTableTest();

    DECLARE_CLASS_NAME(CreateHashTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestHashTableFixSegmentReader();
    void TestKVSegmentOffsetReader();
    void InnerTest(std::string readerType, std::string tableTypeStr);
    void FixInnerTest(common::HashTableAccessType tableType, bool compactHashKey, bool enableTTL, bool isOnline,
                      bool useCompactBucket);
    void OffsetInnerTest(common::HashTableAccessType tableType, bool isMultiRegion, bool compactHashKey, bool enableTTL,
                         bool isOnline, bool isShortOffset);

private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mDocStr;
    config::KVIndexConfigPtr mKVConfig;

private:
    typedef std::map<std::string, std::string> KVMap;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CreateHashTableTest, TestHashTableFixSegmentReader);
INDEXLIB_UNIT_TEST_CASE(CreateHashTableTest, TestKVSegmentOffsetReader);
}} // namespace indexlib::index
