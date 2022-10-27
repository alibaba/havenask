#ifndef __INDEXLIB_CUCKOOHASHTABLEMULTITHREADTEST_H
#define __INDEXLIB_CUCKOOHASHTABLEMULTITHREADTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/hash_table/cuckoo_hash_table.h"

IE_NAMESPACE_BEGIN(common);

class CuckooHashTableMultiThreadTest : public INDEXLIB_TESTBASE
{
public:
    CuckooHashTableMultiThreadTest();
    ~CuckooHashTableMultiThreadTest();

    DECLARE_CLASS_NAME(CuckooHashTableMultiThreadTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMultiThreadWithTs();
    void TestMultiThreadWithOffset();
private:
    IE_LOG_DECLARE();
};

//INDEXLIB_UNIT_TEST_CASE(CuckooHashTableMultiThreadTest, TestMultiThreadWithTs);
//INDEXLIB_UNIT_TEST_CASE(CuckooHashTableMultiThreadTest, TestMultiThreadWithOffset);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CUCKOOHASHTABLEMULTITHREADTEST_H
