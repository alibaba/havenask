#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/key_value_merge_meta_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class KeyValueMergeMetaCreatorTest : public INDEXLIB_TESTBASE
{
public:
    KeyValueMergeMetaCreatorTest();
    ~KeyValueMergeMetaCreatorTest();

    DECLARE_CLASS_NAME(KeyValueMergeMetaCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void CheckMergeMeta(IndexMergeMeta& mergeMeta, const std::string& version, const std::string& mergeTaskItems,
                        const std::string& mergePlanMetas);

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KeyValueMergeMetaCreatorTest, TestSimpleProcess);
}} // namespace indexlib::merger
