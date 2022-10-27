#ifndef __INDEXLIB_SEPARATECHAINHASHTABLETEST_H
#define __INDEXLIB_SEPARATECHAINHASHTABLETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/hash_table/separate_chain_hash_table.h"

IE_NAMESPACE_BEGIN(common);

class SeparateChainHashTableTest : public INDEXLIB_TESTBASE
{
public:
    SeparateChainHashTableTest();
    ~SeparateChainHashTableTest();

    DECLARE_CLASS_NAME(SeparateChainHashTableTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SeparateChainHashTableTest, TestSimpleProcess);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SEPARATECHAINHASHTABLETEST_H
