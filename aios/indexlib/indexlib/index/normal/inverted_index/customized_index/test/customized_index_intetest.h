#ifndef __INDEXLIB_CUSTOMIZEDINDEXINTETEST_H
#define __INDEXLIB_CUSTOMIZEDINDEXINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/plugin/plugin_manager.h"

IE_NAMESPACE_BEGIN(index);

class CustomizedIndexInteTest : public INDEXLIB_TESTBASE
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
    config::IndexPartitionSchemaPtr PrepareCustomIndexSchema(
        const std::string& fieldStr, const std::string& indexStr,
        const std::vector<std::string>& customIndexes);

    void CheckDemoIndexerMeta(const file_system::DirectoryPtr& rootDir,
                              const std::string& segDir,
                              const std::string& indexName,                              
                              const std::vector<std::string> keys,
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
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestIndexRetrieverAsync);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestTwoIndexShareOneIndexer);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestMultiFieldBuild);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestMergeCustomizedIndex);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestIsCompleteMergeFlag);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestEstimateBuildMemUse);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestEstimateMergeMemUse);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestEstimateReopenMemUse);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestEstimateInitMemUse);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestPartitionResourceProviderSupportCustomIndex);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestAddNewCustomIndexWithCustomIndexExist);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexInteTest, TestSearchBuildingData);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_CUSTOMIZEDINDEXINTETEST_H
