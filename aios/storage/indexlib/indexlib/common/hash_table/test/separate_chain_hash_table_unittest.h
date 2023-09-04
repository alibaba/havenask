#ifndef __INDEXLIB_SEPARATECHAINHASHTABLETEST_H
#define __INDEXLIB_SEPARATECHAINHASHTABLETEST_H

#include "indexlib/common/hash_table/separate_chain_hash_table.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

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
}} // namespace indexlib::common

#endif //__INDEXLIB_SEPARATECHAINHASHTABLETEST_H
