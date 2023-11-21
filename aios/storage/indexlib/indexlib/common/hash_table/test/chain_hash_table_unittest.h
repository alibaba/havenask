#pragma once

#include "indexlib/common/hash_table/chain_hash_table.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class ChainHashTableTest : public INDEXLIB_TESTBASE
{
public:
    ChainHashTableTest();
    ~ChainHashTableTest();

    DECLARE_CLASS_NAME(ChainHashTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();
    // void TestTableInsertMany();
    // void TestTimestmapValue();
    // void TestFindForReadWrite();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ChainHashTableTest, TestSimpleProcess);
// INDEXLIB_UNIT_TEST_CASE(ChainHashTableTest, TestTableInsertMany);
// INDEXLIB_UNIT_TEST_CASE(ChainHashTableTest, TestTimestmapValue);
// INDEXLIB_UNIT_TEST_CASE(ChainHashTableTest, TestFindForReadWrite);
}} // namespace indexlib::common
