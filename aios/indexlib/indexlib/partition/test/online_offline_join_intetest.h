#ifndef __INDEXLIB_ONLINEOFFLINEJOINTEST_H
#define __INDEXLIB_ONLINEOFFLINEJOINTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/partition/online_partition.h"

IE_NAMESPACE_BEGIN(partition);

class OnlineOfflineJoinTest : public INDEXLIB_TESTBASE
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
    void MakeOnePartitionData(
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const std::string& partRootDir, const std::string& docStrs);

    void InnerTestReOpen(const config::IndexPartitionSchemaPtr& schema,
                         const config::IndexPartitionOptions& options,
                         const std::string& fullDoc,
                         const std::vector<std::vector<std::string>>& buildCmds);
    
private:
    void MakeSchema(bool needSub);

protected:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnlineOfflineJoinTest, TestOfflineEndIndexWithStopTs);
INDEXLIB_UNIT_TEST_CASE(OnlineOfflineJoinTest, TestMultiPartitionOfflineEndIndexWithStopTs);
INDEXLIB_UNIT_TEST_CASE(OnlineOfflineJoinTest, TestCompatibleWithOldVersion);
INDEXLIB_UNIT_TEST_CASE(OnlineOfflineJoinTest, TestPatchLoaderIncConsistentWithRt);
INDEXLIB_UNIT_TEST_CASE(OnlineOfflineJoinTest, TestRedoStrategyForIncConsistentWithRt);
INDEXLIB_UNIT_TEST_CASE(OnlineOfflineJoinTest, TestRedoStrategyForIncConsistentWithRtWhenForceReopen);
INDEXLIB_UNIT_TEST_CASE(OnlineOfflineJoinTest, TestDelOpRedoOptimize);
INDEXLIB_UNIT_TEST_CASE(OnlineOfflineJoinTest, TestDelOpRedoOptimizeWithIncNotConsistentWithRt);
INDEXLIB_UNIT_TEST_CASE(OnlineOfflineJoinTest, TestDeletionMapMergeWithIncNotConsistentWithRt);
INDEXLIB_UNIT_TEST_CASE(OnlineOfflineJoinTest, TestUpdateDocInSubDocScene);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINEOFFLINEJOINTEST_H
