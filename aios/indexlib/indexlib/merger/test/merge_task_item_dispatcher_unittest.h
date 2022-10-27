#ifndef __INDEXLIB_MERGETASKITEMDISPATCHERTEST_H
#define __INDEXLIB_MERGETASKITEMDISPATCHERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/merge_task_item_dispatcher.h"
#include "indexlib/merger/test/segment_directory_creator.h"
#include "indexlib/index_base/merge_task_resource_manager.h"

IE_NAMESPACE_BEGIN(merger);

class MergeTaskItemDispatcherTest : public INDEXLIB_TESTBASE
{
public:
    MergeTaskItemDispatcherTest();
    ~MergeTaskItemDispatcherTest();

    DECLARE_CLASS_NAME(MergeTaskItemDispatcherTest);
public:
    typedef std::map<MergeTaskItemDispatcher::MergeItemIdentify, double> CostMap;
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestDispatchWorkItems();
    
    void TestGetDirSize();
    void TestGetDirSizeWithSubDir();
    void TestGetDirSizeInPackageFile();
    void TestGetDirSizeWithSubDirInPackageFile();
    void TestGetMaxDocCount();

    void TestInitMergeTaskIdentifySubSchema();
    void TestInitMergeTaskIdentify();
    
    void TestSimpleProcessWithSubDoc();
    void TestDispatchMergeTaskItem();
    void TestDispatchMergeTaskItemWithSubDoc();
    void TestSimpleProcessWithTruncate();
    void TestSimpleProcessWithMultiShardingIndex();
private:
    void CheckMergeTaskItemCost(MergeTaskItems& allItems,
                                const std::string& itemName, double expectCost);

    void MakeDataForDir(const std::string& dirPath, 
                        const std::string& fileDespStr);

    void CreateDirAndFile(const std::string &dirPath, 
                          const std::vector<std::string> &fileNames, 
                          std::vector<uint32_t> fileListSize);

    void CreateDirAndFileInPackageFile(
            const std::string &segmentPath, 
            const std::vector<std::string> &fileNames, 
            std::vector<uint32_t> fileListSize);

    config::IndexPartitionSchemaPtr CreateSimpleSchema(bool isSub);
    config::IndexPartitionSchemaPtr CreateSchema();
    void MakeFileBySize(const std::string &segRoot, const std::string &fileStr);    
private:
    std::string mRootDir;
    index_base::MergeTaskResourceManagerPtr mResMgr;
private:
    IE_LOG_DECLARE();
};
//INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestDispatchMergeTaskItemWithSubDoc);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestDispatchWorkItems);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestGetDirSize);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestGetDirSizeInPackageFile);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestGetDirSizeWithSubDir);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestGetDirSizeWithSubDirInPackageFile);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestGetMaxDocCount);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestInitMergeTaskIdentifySubSchema);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestInitMergeTaskIdentify);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestSimpleProcessWithSubDoc);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestDispatchMergeTaskItem);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestSimpleProcessWithTruncate);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemDispatcherTest, TestSimpleProcessWithMultiShardingIndex);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGETASKITEMDISPATCHERTEST_H
