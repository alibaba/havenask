#ifndef __INDEXLIB_CUSTOMPARTITIONMERGERTEST_H
#define __INDEXLIB_CUSTOMPARTITIONMERGERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/custom_partition_merger.h"

DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);

IE_NAMESPACE_BEGIN(merger);

class CustomPartitionMergerTest : public INDEXLIB_TESTBASE
{
public:
    CustomPartitionMergerTest();
    ~CustomPartitionMergerTest();

    DECLARE_CLASS_NAME(CustomPartitionMergerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateCustomPartitionMerger();
    void TestPrepareMerge();
    void TestCreateNewVersion();
    void TestDoNothingMergeStrategy();
    void TestCreateMergeMetaWithNoMergePolicy();
    void TestCreateMergeMetaWithMergePolicy();
    void TestCreateTableMergePlanMeta();
    void TestSimpleProcess();
    void TestDoMergeWithEmptyMergePlan();
    void TestMultiPartitionMerge();
    void TestUseSpecifiedDeployFileList();
    void TestValidateMergeTaskDescriptions();
    void TestEndMergeFailOver();

private:
    void PrepareBuildData(
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const std::string& docString,
        const std::string& rootDir);
    
    void VerifyCreateTableMergePlanMeta(
        const std::vector<int64_t>& timestamps,
        const std::vector<std::vector<int64_t>>& locatorParams,
        int64_t expectTimestamp,
        const std::vector<int64_t>& expectLocator);
    void InnerTestSimpleProcess(bool isUseSpecifiedDeployFileList);
    void InnerTestMultiPartitionMerge(const config::IndexPartitionOptions& options,
                                      const table::TableFactoryWrapperPtr& fakeFactory,
                                      const std::vector<segmentid_t>& targetSegIdList);
    
private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mPluginRoot;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestCreateCustomPartitionMerger);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestPrepareMerge);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestCreateNewVersion);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestDoNothingMergeStrategy);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestCreateMergeMetaWithNoMergePolicy);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestCreateMergeMetaWithMergePolicy);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestCreateTableMergePlanMeta);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestDoMergeWithEmptyMergePlan);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestMultiPartitionMerge);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestValidateMergeTaskDescriptions);
INDEXLIB_UNIT_TEST_CASE(CustomPartitionMergerTest, TestEndMergeFailOver);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_CUSTOMPARTITIONMERGERTEST_H
