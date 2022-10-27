#ifndef __INDEXLIB_SINGLEPARTPATCHFINDERTEST_H
#define __INDEXLIB_SINGLEPARTPATCHFINDERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/patch/single_part_patch_finder.h"

IE_NAMESPACE_BEGIN(index_base);

class SinglePartPatchFinderTest : public INDEXLIB_TESTBASE
{
public:
    SinglePartPatchFinderTest();
    ~SinglePartPatchFinderTest();

    DECLARE_CLASS_NAME(SinglePartPatchFinderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFindDeletionmapFiles();
    void TestFindDeletionmapFilesWithSliceFile();
    void TestFindDeletionmapFilesWithEmptySegment();
    void TestFindAttrPatchFiles();
    void TestFindInPackAttrPatchFiles();
    void TestFindAttrPatchFilesForSegment();
    void TestFindAttrPatchFilesForMergedSegment();
    void TestFindAttrPatchFilesWithException();
    void TestExtractSegmentIdFromAttrPatchFile();

private:
    void CheckDeletePatchInfos(
            segmentid_t segId, const std::string& patchFilePath,
            const DeletePatchInfos& patchInfos);

    void CheckAttrPatchInfos(
            segmentid_t dstSegId, const std::string& patchInfoStr,
            const AttrPatchInfos& patchInfos);

    void CheckAttrPatchInfosForSegment(
            const std::string& patchInfoStr,
            const AttrPatchFileInfoVec& patchInfos);

    SinglePartPatchFinderPtr CreatePatchFinder();

private: 
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SinglePartPatchFinderTest, TestFindDeletionmapFiles);
INDEXLIB_UNIT_TEST_CASE(SinglePartPatchFinderTest, TestFindDeletionmapFilesWithSliceFile);
INDEXLIB_UNIT_TEST_CASE(SinglePartPatchFinderTest, TestFindDeletionmapFilesWithEmptySegment);
INDEXLIB_UNIT_TEST_CASE(SinglePartPatchFinderTest, TestFindAttrPatchFiles);
INDEXLIB_UNIT_TEST_CASE(SinglePartPatchFinderTest, TestFindInPackAttrPatchFiles);
INDEXLIB_UNIT_TEST_CASE(SinglePartPatchFinderTest, TestExtractSegmentIdFromAttrPatchFile);
INDEXLIB_UNIT_TEST_CASE(SinglePartPatchFinderTest, TestFindAttrPatchFilesWithException);
INDEXLIB_UNIT_TEST_CASE(SinglePartPatchFinderTest, TestFindAttrPatchFilesForSegment);
INDEXLIB_UNIT_TEST_CASE(SinglePartPatchFinderTest, TestFindAttrPatchFilesForMergedSegment);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SINGLEPARTPATCHFINDERTEST_H
