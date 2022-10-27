#ifndef __INDEXLIB_DELETIONMAPMERGETEST_H
#define __INDEXLIB_DELETIONMAPMERGETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"
#include "indexlib/merger/segment_directory.h"

IE_NAMESPACE_BEGIN(index);

class DeletionMapMergeTest : public INDEXLIB_TESTBASE
{
public:
    DeletionMapMergeTest();
    ~DeletionMapMergeTest();

    DECLARE_CLASS_NAME(DeletionMapMergeTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForMergeAll();
    void TestCaseForMergePartial();
    void TestCaseForMergeWithMultiVersion();

private:
    merger::SegmentDirectoryPtr MakeFakeDeletionMapData(const std::string& delStr);
    void CreateSegmentMergeInfos(const std::string& mergePlan, 
                                 index_base::SegmentMergeInfos& segMergeInfos);
    merger::SegmentDirectoryPtr CreateDeletionMapData(
            const std::vector<std::string> &delSegments);

private:
    std::string mDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DeletionMapMergeTest, TestCaseForMergeAll);
INDEXLIB_UNIT_TEST_CASE(DeletionMapMergeTest, TestCaseForMergePartial);
INDEXLIB_UNIT_TEST_CASE(DeletionMapMergeTest, TestCaseForMergeWithMultiVersion);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DELETIONMAPMERGETEST_H
