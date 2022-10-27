#ifndef __INDEXLIB_CUCKOOHASHTABLETEST_H
#define __INDEXLIB_CUCKOOHASHTABLETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/hash_table/cuckoo_hash_table.h"

IE_NAMESPACE_BEGIN(common);

class CuckooHashTableTest : public INDEXLIB_TESTBASE
{
public:
    CuckooHashTableTest();
    ~CuckooHashTableTest();

    DECLARE_CLASS_NAME(CuckooHashTableTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCuckooKick();
    void TestTableDeleteFull();
    void TestTableInsertMany();
    void TestTableInsertManyMod16();

    void TestTimestmapValue();
    void TestOffsetValue();
    void TestFindForReadWrite();
    void TestInsertSpecialKey();
    void TestIterator();
    void TestFileIterator();
    void TestShrink();
    void TestShrinkFail();
    void TestIncHashFunc();
    void TestStretch();
    void TestBuildMemoryToCapacity();

    void TestCompress();
    void TestCalculateDeleteCount();

    void TestInitNoIO();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestCuckooKick);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestTableDeleteFull);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestTableInsertMany);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestTableInsertManyMod16);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestTimestmapValue);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestOffsetValue);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestFindForReadWrite);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestInsertSpecialKey);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestIterator);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestFileIterator);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestShrink);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestShrinkFail);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestIncHashFunc);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestStretch);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestBuildMemoryToCapacity);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestCompress);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestCalculateDeleteCount);
INDEXLIB_UNIT_TEST_CASE(CuckooHashTableTest, TestInitNoIO);
IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CUCKOOHASHTABLETEST_H
