#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/kkv/on_disk_separate_chain_hash_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class OnDiskSeparateChainHashIteratorTest : public INDEXLIB_TESTBASE
{
public:
    OnDiskSeparateChainHashIteratorTest();
    ~OnDiskSeparateChainHashIteratorTest();

    DECLARE_CLASS_NAME(OnDiskSeparateChainHashIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void PrepareData(size_t count);
    void CheckData(size_t count);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::KKVIndexConfigPtr mKKVIndexConfig;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnDiskSeparateChainHashIteratorTest, TestSimpleProcess);
}} // namespace indexlib::index
