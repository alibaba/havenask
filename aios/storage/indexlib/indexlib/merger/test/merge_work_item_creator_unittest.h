#pragma once

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/merger/merge_work_item_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class MergeWorkItemCreatorTest : public INDEXLIB_TESTBASE
{
public:
    MergeWorkItemCreatorTest();
    ~MergeWorkItemCreatorTest();

    DECLARE_CLASS_NAME(MergeWorkItemCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateMergeWorkItem();
    void TestCreateKVMergeWorkItem();
    void TestCreateMergeWorkItemForSourceGroup();

private:
    void CheckTaskItemWithMergeItem(segmentid_t targetSegmentId, const MergeTaskItem& taskItem, MergeWorkItem* workItem,
                                    bool needCheckDirectoryExist);

private:
    std::string mRootDir;
    std::string mInstancePath;
    indexlib::file_system::DirectoryPtr mInstanceDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeWorkItemCreatorTest, TestCreateMergeWorkItem);
INDEXLIB_UNIT_TEST_CASE(MergeWorkItemCreatorTest, TestCreateKVMergeWorkItem);
INDEXLIB_UNIT_TEST_CASE(MergeWorkItemCreatorTest, TestCreateMergeWorkItemForSourceGroup);
}} // namespace indexlib::merger
