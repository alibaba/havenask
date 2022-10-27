#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include "indexlib/index_base/patch/test/single_part_patch_finder_unittest.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(partition);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SinglePartPatchFinderTest);

SinglePartPatchFinderTest::SinglePartPatchFinderTest()
{
}

SinglePartPatchFinderTest::~SinglePartPatchFinderTest()
{
}

void SinglePartPatchFinderTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void SinglePartPatchFinderTest::CaseTearDown()
{
}

void SinglePartPatchFinderTest::TestFindDeletionmapFiles()
{
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;2,5,6");

    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_1_level_0/deletionmap/data_0", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_1_level_0/deletionmap/data_1", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_2_level_0/deletionmap/data_1", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_2_level_0/deletionmap/data_2.__tmp__", "");

    SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
    DeletePatchInfos patchInfos;
    patchFinder->FindDeletionMapFiles(patchInfos);

    ASSERT_EQ((size_t)2, patchInfos.size());
    CheckDeletePatchInfos(0, "1,segment_1_level_0/deletionmap/data_0", patchInfos);
    CheckDeletePatchInfos(1, "2,segment_2_level_0/deletionmap/data_1", patchInfos);
}

void SinglePartPatchFinderTest::TestFindDeletionmapFilesWithSliceFile()
{
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;2,5,6");

    DeletionMapReader reader;
    PartitionDataPtr partitionData = 
        PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());
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
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,0,5;2,5,6");

    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_2_level_0/deletionmap/data_0", "");

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
    ASSERT_FALSE(
            PatchFileFinder::ExtractSegmentIdFromAttrPatchFile(
                    "data", srcSegment, destSegment));
    ASSERT_FALSE(
            PatchFileFinder::ExtractSegmentIdFromAttrPatchFile(
                    "1_2.patch__tmp", srcSegment, destSegment));
    ASSERT_FALSE(
            PatchFileFinder::ExtractSegmentIdFromAttrPatchFile(
                    "1b.patch", srcSegment, destSegment));
    ASSERT_FALSE(
            PatchFileFinder::ExtractSegmentIdFromAttrPatchFile(
                    "1_b.patch", srcSegment, destSegment));
    ASSERT_FALSE(
            PatchFileFinder::ExtractSegmentIdFromAttrPatchFile(
                    "1_1_2.patch", srcSegment, destSegment));
    ASSERT_FALSE(
            PatchFileFinder::ExtractSegmentIdFromAttrPatchFile(
                    "1.patch", srcSegment, destSegment));
    ASSERT_TRUE(
            PatchFileFinder::ExtractSegmentIdFromAttrPatchFile(
                    "1_2.patch", srcSegment, destSegment));
    ASSERT_EQ((segmentid_t)1, srcSegment);
    ASSERT_EQ((segmentid_t)2, destSegment);
}

void SinglePartPatchFinderTest::TestFindInPackAttrPatchFiles()
{
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;2,5,6;3,7,8");

    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_1_level_0/attribute/price/subAttr/1_0.patch", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_2_level_0/attribute/price/subAttr/2_0.patch", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_2_level_0/attribute/price/subAttr/2_1.patch", "");

    AttrPatchInfos patchInfos;
    SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
    patchFinder->FindInPackAttrPatchFiles("price", "subAttr", patchInfos);
    ASSERT_EQ((size_t)2, patchInfos.size());
    CheckAttrPatchInfos(0, "1,segment_1_level_0/attribute/price/subAttr/1_0.patch;"
                        "2,segment_2_level_0/attribute/price/subAttr/2_0.patch", patchInfos);
    CheckAttrPatchInfos(1, "2,segment_2_level_0/attribute/price/subAttr/2_1.patch", patchInfos);
}

void SinglePartPatchFinderTest::TestFindAttrPatchFiles()
{
    // schema0: segment0,segment1
    // schema1: segment2,segment3
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,3,0,0,0;1,4,5,1,0;2,5,6,2,1;3,7,8,3,1");

    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_1_level_0/attribute/price/1_0.patch", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_2_level_0/attribute/price/2_0.patch", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_2_level_0/attribute/price/2_1.patch", "");

    {
        AttrPatchInfos patchInfos;
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        patchFinder->FindAttrPatchFiles("price", INVALID_SCHEMA_OP_ID, patchInfos);
        ASSERT_EQ((size_t)2, patchInfos.size());
        CheckAttrPatchInfos(0, "1,segment_1_level_0/attribute/price/1_0.patch;"
                            "2,segment_2_level_0/attribute/price/2_0.patch", patchInfos);
        CheckAttrPatchInfos(1, "2,segment_2_level_0/attribute/price/2_1.patch", patchInfos);
    }
    {
        AttrPatchInfos patchInfos;
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        patchFinder->FindAttrPatchFiles("price", 1, patchInfos);
        ASSERT_EQ((size_t)2, patchInfos.size());
        CheckAttrPatchInfos(0, "2,segment_2_level_0/attribute/price/2_0.patch", patchInfos);
        CheckAttrPatchInfos(1, "2,segment_2_level_0/attribute/price/2_1.patch", patchInfos);
    }
}

void SinglePartPatchFinderTest::TestFindAttrPatchFilesWithException()
{
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;2,5,6");

    AttrPatchInfos patchInfos;
    {
        // test exception with valid patch name, srcSegment = dstSegment
        FileSystemWrapper::AtomicStore(
                mRootDir + "segment_1_level_0/attribute/price/1_1.patch", "");

        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        ASSERT_THROW(patchFinder->FindAttrPatchFiles(
                        "price", INVALID_SCHEMA_OP_ID, patchInfos),
                     IndexCollapsedException);
        FileSystemWrapper::DeleteFile(
                mRootDir + "segment_1_level_0/attribute/price/1_1.patch");
    }

    {
        // test exception with valid patch name, dstSegment > srcSegment
        FileSystemWrapper::AtomicStore(
                mRootDir + "segment_1_level_0/attribute/price/0_1.patch", "");
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        ASSERT_THROW(patchFinder->FindAttrPatchFiles(
                        "price", INVALID_SCHEMA_OP_ID, patchInfos),
                     IndexCollapsedException);
        FileSystemWrapper::DeleteFile(
                mRootDir + "segment_1_level_0/attribute/price/0_1.patch");
    }

    {
        // test exception with duplicate patch srcSegment
        FileSystemWrapper::AtomicStore(
                mRootDir + "segment_1_level_0/attribute/price/1_0.patch", "");
        FileSystemWrapper::AtomicStore(
                mRootDir + "segment_2_level_0/attribute/price/1_0.patch", "");

        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        ASSERT_THROW(patchFinder->FindAttrPatchFiles(
                        "price", INVALID_SCHEMA_OP_ID, patchInfos),
                     IndexCollapsedException);
    }
}

void SinglePartPatchFinderTest::TestFindAttrPatchFilesForSegment()
{
    // schema0: segment0,segment1
    // schema1: segment2,segment3
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,3,0,0,0;1,4,5,1,0;2,5,6,2,1;3,7,8,3,1");

    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_1_level_0/attribute/price/1_0.patch", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_2_level_0/attribute/price/2_0.patch", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_2_level_0/attribute/price/2_1.patch", "");

    {
        AttrPatchFileInfoVec patchInfo;
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        patchFinder->FindAttrPatchFilesForSegment("price", 0, INVALID_SCHEMA_OP_ID, patchInfo);
        CheckAttrPatchInfosForSegment("1,segment_1_level_0/attribute/price/1_0.patch;"
                "2,segment_2_level_0/attribute/price/2_0.patch", patchInfo);

        patchFinder->FindAttrPatchFilesForSegment("price", 1, INVALID_SCHEMA_OP_ID, patchInfo);
        CheckAttrPatchInfosForSegment("2,segment_2_level_0/attribute/price/2_1.patch", patchInfo);
    }
    {
        AttrPatchFileInfoVec patchInfo;
        SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
        patchFinder->FindAttrPatchFilesForSegment("price", 0, 1, patchInfo);
        CheckAttrPatchInfosForSegment("2,segment_2_level_0/attribute/price/2_0.patch", patchInfo);

        patchFinder->FindAttrPatchFilesForSegment("price", 1, INVALID_SCHEMA_OP_ID, patchInfo);
        CheckAttrPatchInfosForSegment("2,segment_2_level_0/attribute/price/2_1.patch", patchInfo);
    }
}

void SinglePartPatchFinderTest::TestFindAttrPatchFilesForMergedSegment()
{
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,3,0;1,4,5;4,7,8");

    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_1_level_0/attribute/price/1_0.patch", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_4_level_0/attribute/price/2_0.patch", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_4_level_0/attribute/price/3_0.patch", "");
    FileSystemWrapper::AtomicStore(
            mRootDir + "segment_4_level_0/attribute/price/3_1.patch", "");

    // make segment_4 merged segment
    string segInfoPath = mRootDir + "segment_4_level_0/segment_info";
    SegmentInfo segInfo;
    segInfo.Load(segInfoPath);
    segInfo.mergedSegment = true;
    FileSystemWrapper::DeleteFile(segInfoPath);
    segInfo.Store(segInfoPath);

    AttrPatchFileInfoVec patchInfo;
    SinglePartPatchFinderPtr patchFinder = CreatePatchFinder();
    patchFinder->FindAttrPatchFilesForSegment("price", 0, INVALID_SCHEMA_OP_ID, patchInfo);
    CheckAttrPatchInfosForSegment(
            "1,segment_1_level_0/attribute/price/1_0.patch;"
            "2,segment_4_level_0/attribute/price/2_0.patch;"
            "3,segment_4_level_0/attribute/price/3_0.patch", patchInfo);

    patchFinder->FindAttrPatchFilesForSegment("price", 1, INVALID_SCHEMA_OP_ID, patchInfo);
    CheckAttrPatchInfosForSegment("3,segment_4_level_0/attribute/price/3_1.patch", patchInfo);
}

void SinglePartPatchFinderTest::CheckDeletePatchInfos(
        segmentid_t segId, const string& patchFilePath,
        const DeletePatchInfos& patchInfos)
{
    vector<string> tmpStrs;
    StringUtil::fromString(patchFilePath, tmpStrs, ",");
    assert(tmpStrs.size() == 2);

    DeletePatchInfos::const_iterator iter = 
        patchInfos.find(segId);
    ASSERT_TRUE(iter != patchInfos.end());

    PatchFileInfo fileInfo = iter->second;
    segmentid_t srcId;
    StringUtil::fromString(tmpStrs[0], srcId);
    ASSERT_EQ(srcId, fileInfo.srcSegment);

    string actPath = PathUtil::JoinPath(
            fileInfo.patchDirectory->GetPath(), 
            fileInfo.patchFileName);
    ASSERT_EQ(mRootDir + tmpStrs[1], actPath);
}

void SinglePartPatchFinderTest::CheckAttrPatchInfosForSegment(
        const std::string& patchInfoStr,
        const AttrPatchFileInfoVec& patchInfo)
{
    vector<vector<string> > tmpStrs;
    StringUtil::fromString(patchInfoStr, tmpStrs, ",", ";");
    ASSERT_EQ(tmpStrs.size(), patchInfo.size());
    for (size_t i = 0; i < patchInfo.size(); i++)
    {
        assert(tmpStrs[i].size() == 2);
        segmentid_t srcId;
        StringUtil::fromString(tmpStrs[i][0], srcId);
        ASSERT_EQ(srcId, patchInfo[i].srcSegment);

        const string& expectPatchPath = tmpStrs[i][1];
        string actualPath = PathUtil::JoinPath(
                patchInfo[i].patchDirectory->GetPath(),
                patchInfo[i].patchFileName);

        ASSERT_EQ(mRootDir + expectPatchPath, actualPath);
    }
}

// patchInfoStr:
// srcId0,patchFilePath0;srcId1,patchFilePath1;...
void SinglePartPatchFinderTest::CheckAttrPatchInfos(
        segmentid_t dstSegId,
        const string& patchInfoStr,
        const AttrPatchInfos& patchInfos)
{
    AttrPatchInfos::const_iterator iter = 
        patchInfos.find(dstSegId);
    ASSERT_TRUE(iter != patchInfos.end());

    const AttrPatchFileInfoVec& patchInfo = iter->second;
    CheckAttrPatchInfosForSegment(patchInfoStr, patchInfo);
}

SinglePartPatchFinderPtr SinglePartPatchFinderTest::CreatePatchFinder()
{
    OnDiskPartitionDataPtr partitionData = 
        PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());
    SegmentDirectoryPtr segmentDirectory = partitionData->GetSegmentDirectory();

    SinglePartPatchFinderPtr patchFinder(new SinglePartPatchFinder);
    patchFinder->Init(segmentDirectory);
    return patchFinder;
}

IE_NAMESPACE_END(index_base);

