#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/separate_chain_prefix_key_table.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SeparateChainPrefixKeyTableTest : public INDEXLIB_TESTBASE
{
public:
    SeparateChainPrefixKeyTableTest();
    ~SeparateChainPrefixKeyTableTest();

    DECLARE_CLASS_NAME(SeparateChainPrefixKeyTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestReadAndWriteTable();
    void TestRWTable();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SeparateChainPrefixKeyTableTest, TestReadAndWriteTable);
INDEXLIB_UNIT_TEST_CASE(SeparateChainPrefixKeyTableTest, TestRWTable);
}} // namespace indexlib::index
