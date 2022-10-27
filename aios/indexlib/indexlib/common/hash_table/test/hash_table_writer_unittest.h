#ifndef __INDEXLIB_HASHTABLEWRITERTEST_H
#define __INDEXLIB_HASHTABLEWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/hash_table/hash_table_writer.h"

IE_NAMESPACE_BEGIN(common);

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

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_HASHTABLEWRITERTEST_H
