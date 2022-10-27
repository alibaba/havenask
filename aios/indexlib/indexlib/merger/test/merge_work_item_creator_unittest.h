#ifndef __INDEXLIB_MERGEWORKITEMCREATORTEST_H
#define __INDEXLIB_MERGEWORKITEMCREATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/merge_work_item_creator.h"

IE_NAMESPACE_BEGIN(merger);

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

private:
    void CheckTaskItemWithMergeItem(
        segmentid_t targetSegmentId, const MergeTaskItem &taskItem,
        MergeWorkItem * workItem, bool needCheckDirectoryExist);
private:
    std::string mRootDir;
    std::string mInstanceDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeWorkItemCreatorTest, TestCreateMergeWorkItem);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGEWORKITEMCREATORTEST_H
