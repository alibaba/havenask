#ifndef __INDEXLIB_HASHTABLEWRITERTEST_H
#define __INDEXLIB_HASHTABLEWRITERTEST_H

#include "indexlib/common/hash_table/hash_table_writer.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class HashTableWriterTest : public INDEXLIB_TESTBASE
{
public:
    HashTableWriterTest();
    ~HashTableWriterTest();

    DECLARE_CLASS_NAME(HashTableWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HashTableWriterTest, TestSimpleProcess);
}} // namespace indexlib::common

#endif //__INDEXLIB_HASHTABLEWRITERTEST_H
