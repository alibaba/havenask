#ifndef __INDEXLIB_HASHTABLEREADERTEST_H
#define __INDEXLIB_HASHTABLEREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/hash_table/hash_table_reader.h"

IE_NAMESPACE_BEGIN(common);

class HashTableReaderTest : public INDEXLIB_TESTBASE
{
public:
    HashTableReaderTest();
    ~HashTableReaderTest();

    DECLARE_CLASS_NAME(HashTableReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestException();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HashTableReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(HashTableReaderTest, TestException);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_HASHTABLEREADERTEST_H
