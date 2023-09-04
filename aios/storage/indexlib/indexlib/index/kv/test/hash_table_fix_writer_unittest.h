#ifndef __INDEXLIB_HASHTABLEFIXWRITERTEST_H
#define __INDEXLIB_HASHTABLEFIXWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/kv/hash_table_fix_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class HashTableFixWriterTest : public INDEXLIB_TESTBASE
{
public:
    HashTableFixWriterTest();
    ~HashTableFixWriterTest();

    DECLARE_CLASS_NAME(HashTableFixWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddDocument();

private:
    template <typename T>
    void InnerTestAddDocument();

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::KVIndexConfigPtr mKVConfig;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HashTableFixWriterTest, TestAddDocument);
}} // namespace indexlib::index

#endif //__INDEXLIB_HASHTABLEFIXWRITERTEST_H
