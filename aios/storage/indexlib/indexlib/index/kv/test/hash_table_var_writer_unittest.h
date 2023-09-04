#ifndef __INDEXLIB_HASHTABLEVARWRITERTEST_H
#define __INDEXLIB_HASHTABLEVARWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/kv/hash_table_var_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class HashTableVarWriterTest : public INDEXLIB_TESTBASE
{
public:
    HashTableVarWriterTest();
    ~HashTableVarWriterTest();

    DECLARE_CLASS_NAME(HashTableVarWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCalculateMemoryRatio();
    void TestValueThresholdWithShortenOffset();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HashTableVarWriterTest, TestCalculateMemoryRatio);
INDEXLIB_UNIT_TEST_CASE(HashTableVarWriterTest, TestValueThresholdWithShortenOffset);
}} // namespace indexlib::index

#endif //__INDEXLIB_HASHTABLEVARWRITERTEST_H
