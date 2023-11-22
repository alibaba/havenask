#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OnlineOfflineJoinTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    OnlineOfflineJoinTest();
    ~OnlineOfflineJoinTest();

    DECLARE_CLASS_NAME(OnlineOfflineJoinTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOfflineEndIndexWithStopTs();
    void TestMultiPartitionOfflineEndIndexWithStopTs();
    void TestCompatibleWithOldVersion();
    void TestPatchLoaderIncConsistentWithRt();
    void TestRedoStrategyForIncConsistentWithRt();
    void TestRedoStrategyForIncConsistentWithRtWhenForceReopen();
    void TestDelOpRedoOptimize();
    void TestDelOpRedoOptimizeWithIncNotConsistentWithRt();
    void TestDeletionMapMergeWithIncNotConsistentWithRt();
    void TestUpdateDocInSubDocScene();

private:
    void MakeOnePartitionData(const config::IndexPartitionSchemaPtr& schema,
                              const config::IndexPartitionOptions& options, const std::string& partRootDir,
                              const std::string& docStrs);

    void InnerTestReOpen(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                         const std::string& fullDoc, const std::vector<std::vector<std::string>>& buildCmds);

private:
    void MakeSchema(bool needSub);

protected:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, OnlineOfflineJoinTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineOfflineJoinTest, TestOfflineEndIndexWithStopTs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineOfflineJoinTest, TestMultiPartitionOfflineEndIndexWithStopTs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineOfflineJoinTest, TestCompatibleWithOldVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineOfflineJoinTest, TestPatchLoaderIncConsistentWithRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineOfflineJoinTest, TestRedoStrategyForIncConsistentWithRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineOfflineJoinTest, TestRedoStrategyForIncConsistentWithRtWhenForceReopen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineOfflineJoinTest, TestDelOpRedoOptimize);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineOfflineJoinTest, TestDelOpRedoOptimizeWithIncNotConsistentWithRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineOfflineJoinTest, TestDeletionMapMergeWithIncNotConsistentWithRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineOfflineJoinTest, TestUpdateDocInSubDocScene);
}} // namespace indexlib::partition
