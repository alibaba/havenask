#ifndef __INDEXLIB_CUSTOMIZEDINDEXINTETEST_H
#define __INDEXLIB_CUSTOMIZEDINDEXINTETEST_H

#include "indexlib/common_define.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class CustomizedIndexInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    CustomizedIndexInteTest();
    ~CustomizedIndexInteTest();

    DECLARE_CLASS_NAME(CustomizedIndexInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestIndexRetrieverAsync();
    void TestTwoIndexShareOneIndexer();
    void TestMultiFieldBuild();
    void TestMergeCustomizedIndex();
    void TestIsCompleteMergeFlag();
    void TestEstimateBuildMemUse();
    void TestEstimateMergeMemUse();
    void TestEstimateReopenMemUse();
    void TestEstimateInitMemUse();
    void TestPartitionResourceProviderSupportCustomIndex();
    void TestAddNewCustomIndexWithCustomIndexExist();
    void TestSearchBuildingData();

private:
    config::IndexPartitionSchemaPtr PrepareCustomIndexSchema(const std::string& fieldStr, const std::string& indexStr,
                                                             const std::vector<std::string>& customIndexes);

    void CheckDemoIndexerMeta(const file_system::DirectoryPtr& rootDir, const std::string& segDir,
                              const std::string& indexName, const std::vector<std::string> keys,
                              const std::vector<std::string> values);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

private:
    plugin::PluginManagerPtr mPluginManager;
    util::CounterMapPtr mCounterMap;
    partition::IndexPartitionResource mPartitionResource;
};
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestIndexRetrieverAsync);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestTwoIndexShareOneIndexer);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestMultiFieldBuild);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestMergeCustomizedIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestIsCompleteMergeFlag);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestEstimateBuildMemUse);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestEstimateMergeMemUse);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestEstimateReopenMemUse);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestEstimateInitMemUse);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestPartitionResourceProviderSupportCustomIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestAddNewCustomIndexWithCustomIndexExist);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CustomizedIndexInteTest, TestSearchBuildingData);

INSTANTIATE_TEST_CASE_P(BuildMode, CustomizedIndexInteTest, testing::Values(0, 1, 2));
}} // namespace indexlib::index

#endif //__INDEXLIB_CUSTOMIZEDINDEXINTETEST_H
