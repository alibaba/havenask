#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/closed_hash_prefix_key_table.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class ClosedHashPrefixKeyTableTest : public INDEXLIB_TESTBASE
{
public:
    ClosedHashPrefixKeyTableTest();
    ~ClosedHashPrefixKeyTableTest();

    DECLARE_CLASS_NAME(ClosedHashPrefixKeyTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestReadAndWriteTable();
    void TestRWTable();

private:
    template <PKeyTableType Type>
    void CheckReadAndWriteTable();
    template <PKeyTableType Type>
    void CheckRWTable();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ClosedHashPrefixKeyTableTest, TestReadAndWriteTable);
INDEXLIB_UNIT_TEST_CASE(ClosedHashPrefixKeyTableTest, TestRWTable);
}} // namespace indexlib::index
