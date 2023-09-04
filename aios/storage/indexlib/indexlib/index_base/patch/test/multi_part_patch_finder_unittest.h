#ifndef __INDEXLIB_MULTIPARTPATCHFINDERTEST_H
#define __INDEXLIB_MULTIPARTPATCHFINDERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index_base/patch/multi_part_patch_finder.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class MultiPartPatchFinderTest : public INDEXLIB_TESTBASE
{
public:
    MultiPartPatchFinderTest();
    ~MultiPartPatchFinderTest();

    DECLARE_CLASS_NAME(MultiPartPatchFinderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFindDeletionmapFiles();
    void TestFindAttrPatchFiles();
    void TestFindAttrPatchFilesWithException();
    void TestFindAttrPatchFilesInSegment();
    void TestFindAttrPatchFilesInSegmentWithException();
    void TestFindAttrPatchFilesForSegment();

private:
    MultiPartSegmentDirectoryPtr CreateMultiPartSegmentDirectory(const std::string& multiPartSegStrs);

    void CheckDeletionMapPatchInfo(segmentid_t virtualSegId, const std::string& expectPath,
                                   const DeletePatchInfos& patchInfos);

    void CheckPatchInfos(segmentid_t dstSegId, const std::string& patchInfoStr, const PatchInfos& patchInfos);

    void CheckAttrPatchInfoForSegment(const std::string& patchInfoStr, const PatchFileInfoVec& patchInfo);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiPartPatchFinderTest, TestFindDeletionmapFiles);
INDEXLIB_UNIT_TEST_CASE(MultiPartPatchFinderTest, TestFindAttrPatchFiles);
INDEXLIB_UNIT_TEST_CASE(MultiPartPatchFinderTest, TestFindAttrPatchFilesWithException);
INDEXLIB_UNIT_TEST_CASE(MultiPartPatchFinderTest, TestFindAttrPatchFilesInSegment);
INDEXLIB_UNIT_TEST_CASE(MultiPartPatchFinderTest, TestFindAttrPatchFilesInSegmentWithException);
INDEXLIB_UNIT_TEST_CASE(MultiPartPatchFinderTest, TestFindAttrPatchFilesForSegment);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_MULTIPARTPATCHFINDERTEST_H
