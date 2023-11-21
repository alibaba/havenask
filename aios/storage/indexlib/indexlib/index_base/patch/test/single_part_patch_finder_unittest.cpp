#include "indexlib/index_base/patch/test/single_part_patch_finder_unittest.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::test;
using namespace indexlib::util;
using namespace indexlib::partition;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SinglePartPatchFinderTest);

SinglePartPatchFinderTest::SinglePartPatchFinderTest() {}

SinglePartPatchFinderTest::~SinglePartPatchFinderTest() {}

void SinglePartPatchFinderTest::CaseSetUp() {}

void SinglePartPatchFinderTest::CaseTearDown() {}

void SinglePartPatchFinderTest::TestFindDeletionmapFiles()
{
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;2,5,6");

    GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/deletionmap/data_0", "");
    GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/deletionmap/data_1", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/deletionmap/data_1", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/deletionmap/data_2.__tmp__", "");

    SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
    DeletePatchInfos patchInfos;
    patchFinder->FindDeletionMapFiles(patchInfos);

    ASSERT_EQ((size_t)2, patchInfos.size());
    CheckDeletePatchInfos(0, "1,segment_1_level_0/deletionmap/data_0", patchInfos);
    CheckDeletePatchInfos(1, "2,segment_2_level_0/deletionmap/data_1", patchInfos);
}

void SinglePartPatchFinderTest::TestFindDeletionmapFilesWithSliceFile()
{
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;2,5,6");

    DeletionMapReader reader;
    PartitionDataPtr partitionData = PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());
    reader.Open(partitionData.get());

    SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
    DeletePatchInfos patchInfos;
    patchFinder->FindDeletionMapFiles(patchInfos);
    ASSERT_EQ((size_t)3, patchInfos.size());

    // load slice file from non-exist file when partion open
    CheckDeletePatchInfos(0, "0,segment_0_level_0/deletionmap/data_0", patchInfos);
    CheckDeletePatchInfos(1, "1,segment_1_level_0/deletionmap/data_1", patchInfos);
    CheckDeletePatchInfos(2, "2,segment_2_level_0/deletionmap/data_2", patchInfos);
}

void SinglePartPatchFinderTest::TestFindDeletionmapFilesWithEmptySegment()
{
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,0,5;2,5,6");

    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/deletionmap/data_0", "");

    SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
    DeletePatchInfos patchInfos;
    patchFinder->FindDeletionMapFiles(patchInfos);

    ASSERT_EQ((size_t)1, patchInfos.size());
    CheckDeletePatchInfos(0, "2,segment_2_level_0/deletionmap/data_0", patchInfos);
}

void SinglePartPatchFinderTest::TestExtractSegmentIdFromAttrPatchFile()
{
    segmentid_t srcSegment;
    segmentid_t destSegment;
    ASSERT_FALSE(PatchFileFinder::ExtractSegmentIdFromPatchFile("data", &srcSegment, &destSegment));
    ASSERT_FALSE(PatchFileFinder::ExtractSegmentIdFromPatchFile("1_2.patch__tmp", &srcSegment, &destSegment));
    ASSERT_FALSE(PatchFileFinder::ExtractSegmentIdFromPatchFile("1b.patch", &srcSegment, &destSegment));
    ASSERT_FALSE(PatchFileFinder::ExtractSegmentIdFromPatchFile("1_b.patch", &srcSegment, &destSegment));
    ASSERT_FALSE(PatchFileFinder::ExtractSegmentIdFromPatchFile("1_1_2.patch", &srcSegment, &destSegment));
    ASSERT_FALSE(PatchFileFinder::ExtractSegmentIdFromPatchFile("1.patch", &srcSegment, &destSegment));
    ASSERT_TRUE(PatchFileFinder::ExtractSegmentIdFromPatchFile("1_2.patch", &srcSegment, &destSegment));
    ASSERT_EQ((segmentid_t)1, srcSegment);
    ASSERT_EQ((segmentid_t)2, destSegment);
}

void SinglePartPatchFinderTest::TestFindInPackAttrPatchFiles()
{
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;2,5,6;3,7,8");

    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0/attribute/price/subAttr/");
    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_2_level_0/attribute/price/subAttr/");
    GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/attribute/price/subAttr/1_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/attribute/price/subAttr/2_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/attribute/price/subAttr/2_1.patch", "");

    PatchInfos patchInfos;
    SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
    patchFinder->FindPatchFiles(PatchFileFinder::PatchType::ATTRIBUTE, "price/subAttr", INVALID_SCHEMA_OP_ID,
                                &patchInfos);
    ASSERT_EQ((size_t)2, patchInfos.size());
    CheckPatchInfos(0,
                    "1,segment_1_level_0/attribute/price/subAttr/1_0.patch;"
                    "2,segment_2_level_0/attribute/price/subAttr/2_0.patch",
                    patchInfos);
    CheckPatchInfos(1, "2,segment_2_level_0/attribute/price/subAttr/2_1.patch", patchInfos);
}

void SinglePartPatchFinderTest::TestFindAttrPatchFiles()
{
    // schema0: segment0,segment1
    // schema1: segment2,segment3
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(),
                                               "0,3,0,0,0;1,4,5,1,0;2,5,6,2,1;3,7,8,3,1");

    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0/attribute/price/");
    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_2_level_0/attribute/price/");
    GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/attribute/price/1_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/attribute/price/2_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/attribute/price/2_1.patch", "");

    {
        PatchInfos patchInfos;
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        patchFinder->FindPatchFiles(PatchFileFinder::PatchType::ATTRIBUTE, "price", INVALID_SCHEMA_OP_ID, &patchInfos);
        ASSERT_EQ((size_t)2, patchInfos.size());
        CheckPatchInfos(0,
                        "1,segment_1_level_0/attribute/price/1_0.patch;"
                        "2,segment_2_level_0/attribute/price/2_0.patch",
                        patchInfos);
        CheckPatchInfos(1, "2,segment_2_level_0/attribute/price/2_1.patch", patchInfos);
    }
    {
        PatchInfos patchInfos;
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        patchFinder->FindPatchFiles(PatchFileFinder::PatchType::ATTRIBUTE, "price", 1, &patchInfos);
        ASSERT_EQ((size_t)2, patchInfos.size());
        CheckPatchInfos(0, "2,segment_2_level_0/attribute/price/2_0.patch", patchInfos);
        CheckPatchInfos(1, "2,segment_2_level_0/attribute/price/2_1.patch", patchInfos);
    }
}

void SinglePartPatchFinderTest::TestFindAttrPatchFilesWithException()
{
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;2,5,6");

    PatchInfos patchInfos;
    {
        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0/attribute/price/");
        // test exception with valid patch name, srcSegment = dstSegment
        GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/attribute/price/1_1.patch", "");

        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        ASSERT_THROW(patchFinder->FindPatchFiles(PatchFileFinder::PatchType::ATTRIBUTE, "price", INVALID_SCHEMA_OP_ID,
                                                 &patchInfos),
                     IndexCollapsedException);
        GET_PARTITION_DIRECTORY()->RemoveFile("segment_1_level_0/attribute/price/1_1.patch");
    }

    {
        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0/attribute/price/");
        // test exception with valid patch name, dstSegment > srcSegment
        GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/attribute/price/0_1.patch", "");
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        ASSERT_THROW(patchFinder->FindPatchFiles(PatchFileFinder::PatchType::ATTRIBUTE, "price", INVALID_SCHEMA_OP_ID,
                                                 &patchInfos),
                     IndexCollapsedException);
        GET_PARTITION_DIRECTORY()->RemoveFile("segment_1_level_0/attribute/price/0_1.patch");
    }

    {
        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0/attribute/price/");
        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_2_level_0/attribute/price/");
        // test exception with duplicate patch srcSegment
        GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/attribute/price/1_0.patch", "");
        GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/attribute/price/1_0.patch", "");

        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        ASSERT_THROW(patchFinder->FindPatchFiles(PatchFileFinder::PatchType::ATTRIBUTE, "price", INVALID_SCHEMA_OP_ID,
                                                 &patchInfos),
                     IndexCollapsedException);
    }
}

void SinglePartPatchFinderTest::TestFindAttrPatchFilesForSegment()
{
    // schema0: segment0,segment1
    // schema1: segment2,segment3
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(),
                                               "0,3,0,0,0;1,4,5,1,0;2,5,6,2,1;3,7,8,3,1");

    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0/attribute/price/");
    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_2_level_0/attribute/price/");
    GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/attribute/price/1_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/attribute/price/2_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/attribute/price/2_1.patch", "");

    {
        PatchFileInfoVec patchInfo;
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::ATTRIBUTE, "price", 0,
                                                    INVALID_SCHEMA_OP_ID, &patchInfo);
        CheckPatchInfosForSegment("1,segment_1_level_0/attribute/price/1_0.patch;"
                                  "2,segment_2_level_0/attribute/price/2_0.patch",
                                  patchInfo);

        patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::ATTRIBUTE, "price", 1,
                                                    INVALID_SCHEMA_OP_ID, &patchInfo);
        CheckPatchInfosForSegment("2,segment_2_level_0/attribute/price/2_1.patch;", patchInfo);
    }
    {
        PatchFileInfoVec patchInfo;
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::ATTRIBUTE, "price", 0, 1, &patchInfo);
        CheckPatchInfosForSegment("2,segment_2_level_0/attribute/price/2_0.patch", patchInfo);

        patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::ATTRIBUTE, "price", 1,
                                                    INVALID_SCHEMA_OP_ID, &patchInfo);
        CheckPatchInfosForSegment("2,segment_2_level_0/attribute/price/2_1.patch", patchInfo);
    }
}

void SinglePartPatchFinderTest::TestFindIndexPatchFilesForSegment()
{
    // schema0: segment0,segment1
    // schema1: segment2,segment3
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(),
                                               "0,3,0,0,0;1,4,5,1,0;2,5,6,2,1;3,7,8,3,1");

    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0/index/price/");
    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_2_level_0/index/price/");
    GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/index/price/1_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/index/price/2_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/index/price/2_1.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/index/price/2_2.patch", "");

    {
        PatchFileInfoVec patchInfo;
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::INDEX, "price", 0, INVALID_SCHEMA_OP_ID,
                                                    &patchInfo);
        CheckPatchInfosForSegment("1,segment_1_level_0/index/price/1_0.patch;"
                                  "2,segment_2_level_0/index/price/2_0.patch",
                                  patchInfo);

        patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::INDEX, "price", 1, INVALID_SCHEMA_OP_ID,
                                                    &patchInfo);
        CheckPatchInfosForSegment("2,segment_2_level_0/index/price/2_1.patch;", patchInfo);

        patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::INDEX, "price", 2, INVALID_SCHEMA_OP_ID,
                                                    &patchInfo);
        CheckPatchInfosForSegment("2,segment_2_level_0/index/price/2_2.patch;", patchInfo);
    }
    {
        PatchFileInfoVec patchInfo;
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::INDEX, "price", 0, 1, &patchInfo);
        CheckPatchInfosForSegment("2,segment_2_level_0/index/price/2_0.patch", patchInfo);

        patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::INDEX, "price", 1, INVALID_SCHEMA_OP_ID,
                                                    &patchInfo);
        CheckPatchInfosForSegment("2,segment_2_level_0/index/price/2_1.patch", patchInfo);

        patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::INDEX, "price", 2, INVALID_SCHEMA_OP_ID,
                                                    &patchInfo);
        CheckPatchInfosForSegment("2,segment_2_level_0/index/price/2_2.patch", patchInfo);
    }
}

void SinglePartPatchFinderTest::TestFindAttrPatchFilesForMergedSegment()
{
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;4,7,8");

    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0/attribute/price/");
    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_4_level_0/attribute/price/");
    GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/attribute/price/1_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_4_level_0/attribute/price/2_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_4_level_0/attribute/price/3_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_4_level_0/attribute/price/3_1.patch", "");

    // make segment_4 merged segment
    auto segDir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_4_level_0", false);
    ASSERT_NE(segDir, nullptr);
    SegmentInfo segInfo;
    segInfo.Load(segDir);
    segInfo.mergedSegment = true;
    segDir->RemoveFile("segment_info");
    segInfo.Store(segDir);

    PatchFileInfoVec patchInfo;
    SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
    patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::ATTRIBUTE, "price", 0, INVALID_SCHEMA_OP_ID,
                                                &patchInfo);
    CheckPatchInfosForSegment("1,segment_1_level_0/attribute/price/1_0.patch;"
                              "2,segment_4_level_0/attribute/price/2_0.patch;"
                              "3,segment_4_level_0/attribute/price/3_0.patch",
                              patchInfo);

    patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::ATTRIBUTE, "price", 1, INVALID_SCHEMA_OP_ID,
                                                &patchInfo);
    CheckPatchInfosForSegment("3,segment_4_level_0/attribute/price/3_1.patch", patchInfo);
}

void SinglePartPatchFinderTest::TestFindIndexPatchFilesForMergedSegment()
{
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;4,7,8");

    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0/index/price/");
    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_4_level_0/index/price/");
    GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/index/price/1_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/index/price/1_1.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_4_level_0/index/price/2_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_4_level_0/index/price/3_0.patch", "");
    GET_PARTITION_DIRECTORY()->Store("segment_4_level_0/index/price/3_1.patch", "");

    // make segment_4 merged segment
    auto segDir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_4_level_0", false);
    ASSERT_NE(segDir, nullptr);
    SegmentInfo segInfo;
    segInfo.Load(segDir);
    segInfo.mergedSegment = true;
    segDir->RemoveFile("segment_info");
    segInfo.Store(segDir);

    PatchFileInfoVec patchInfo;
    SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
    patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::INDEX, "price", 0, INVALID_SCHEMA_OP_ID,
                                                &patchInfo);
    CheckPatchInfosForSegment("1,segment_1_level_0/index/price/1_0.patch;"
                              "2,segment_4_level_0/index/price/2_0.patch;"
                              "3,segment_4_level_0/index/price/3_0.patch",
                              patchInfo);

    patchFinder->FindPatchFilesForTargetSegment(PatchFileFinder::PatchType::INDEX, "price", 1, INVALID_SCHEMA_OP_ID,
                                                &patchInfo);
    CheckPatchInfosForSegment("1,segment_1_level_0/index/price/1_1.patch;"
                              "3,segment_4_level_0/index/price/3_1.patch",
                              patchInfo);
}

void SinglePartPatchFinderTest::CheckDeletePatchInfos(segmentid_t segId, const string& patchFilePath,
                                                      const DeletePatchInfos& patchInfos)
{
    vector<string> tmpStrs;
    StringUtil::fromString(patchFilePath, tmpStrs, ",");
    assert(tmpStrs.size() == 2);

    DeletePatchInfos::const_iterator iter = patchInfos.find(segId);
    ASSERT_TRUE(iter != patchInfos.end());

    PatchFileInfo fileInfo = iter->second;
    segmentid_t srcId;
    StringUtil::fromString(tmpStrs[0], srcId);
    ASSERT_EQ(srcId, fileInfo.srcSegment);

    string actPath = PathUtil::JoinPath(fileInfo.patchDirectory->GetLogicalPath(), fileInfo.patchFileName);
    ASSERT_EQ(tmpStrs[1], actPath);
}

void SinglePartPatchFinderTest::CheckPatchInfosForSegment(const std::string& patchInfoStr,
                                                          const PatchFileInfoVec& patchInfo)
{
    vector<vector<string>> tmpStrs;
    StringUtil::fromString(patchInfoStr, tmpStrs, ",", ";");
    ASSERT_EQ(tmpStrs.size(), patchInfo.size()) << patchInfoStr;
    for (size_t i = 0; i < patchInfo.size(); i++) {
        assert(tmpStrs[i].size() == 2);
        segmentid_t srcId;
        StringUtil::fromString(tmpStrs[i][0], srcId);
        ASSERT_EQ(srcId, patchInfo[i].srcSegment);

        const string& expectPatchPath = tmpStrs[i][1];
        string actualPath =
            PathUtil::JoinPath(patchInfo[i].patchDirectory->GetLogicalPath(), patchInfo[i].patchFileName);

        ASSERT_EQ(expectPatchPath, actualPath);
    }
}

// patchInfoStr:
// srcId0,patchFilePath0;srcId1,patchFilePath1;...
void SinglePartPatchFinderTest::CheckPatchInfos(segmentid_t dstSegId, const string& patchInfoStr,
                                                const PatchInfos& patchInfos)
{
    PatchInfos::const_iterator iter = patchInfos.find(dstSegId);
    ASSERT_TRUE(iter != patchInfos.end());

    const PatchFileInfoVec& patchInfo = iter->second;
    CheckPatchInfosForSegment(patchInfoStr, patchInfo);
}

SinglePartPatchFinderPtr SinglePartPatchFinderTest::CreatePatchFinder()
{
    OnDiskPartitionDataPtr partitionData = PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());
    SegmentDirectoryPtr segmentDirectory = partitionData->GetSegmentDirectory();

    SinglePartPatchFinderPtr patchFinder(new SinglePartPatchFinder);
    patchFinder->Init(segmentDirectory);
    return patchFinder;
}
}} // namespace indexlib::index_base
