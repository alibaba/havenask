#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include "indexlib/index_base/patch/test/multi_part_patch_finder_unittest.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/path_util.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, MultiPartPatchFinderTest);

MultiPartPatchFinderTest::MultiPartPatchFinderTest()
{
}

MultiPartPatchFinderTest::~MultiPartPatchFinderTest()
{
}

void MultiPartPatchFinderTest::CaseSetUp()
{
}

void MultiPartPatchFinderTest::CaseTearDown()
{
}

void MultiPartPatchFinderTest::TestFindDeletionmapFiles()
{
    MultiPartSegmentDirectoryPtr multiSegDir = 
        CreateMultiPartSegmentDirectory("0,0,0;2,0,0;3,0,0##1,0,0;2,0,0;5,0,0");

    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "0/segment_2_level_0/deletionmap/data_0", "");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "0/segment_3_level_0/deletionmap/data_0", "");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "0/segment_3_level_0/deletionmap/data_2", "");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "2/segment_2_level_0/deletionmap/data_0", "");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "2/segment_2_level_0/deletionmap/data_1", "");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "2/segment_5_level_0/deletionmap/data_2", "");


    MultiPartPatchFinder patchFinder;
    patchFinder.Init(multiSegDir);

    DeletePatchInfos patchInfos;
    patchFinder.FindDeletionMapFiles(patchInfos);
    
    ASSERT_EQ((size_t)4, patchInfos.size());

    CheckDeletionMapPatchInfo(0, "2,0/segment_3_level_0/deletionmap/data_0", patchInfos);
    CheckDeletionMapPatchInfo(1, "2,0/segment_3_level_0/deletionmap/data_2", patchInfos);
    CheckDeletionMapPatchInfo(3, "4,2/segment_2_level_0/deletionmap/data_1", patchInfos);
    CheckDeletionMapPatchInfo(4, "5,2/segment_5_level_0/deletionmap/data_2", patchInfos);
}

void MultiPartPatchFinderTest::TestFindAttrPatchFiles()
{
    MultiPartSegmentDirectoryPtr multiSegDir = 
        CreateMultiPartSegmentDirectory("0,0,0;2,0,0;3,0,0##1,0,0;2,0,0;5,0,0");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "0/segment_2_level_0/attribute/price/2_0.patch", "");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "0/segment_3_level_0/attribute/price/3_2.patch", "");

    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "2/segment_2_level_0/attribute/price/2_1.patch", "");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "2/segment_5_level_0/attribute/price/5_1.patch", "");

    MultiPartPatchFinder patchFinder;
    patchFinder.Init(multiSegDir);

    AttrPatchInfos patchInfos;
    patchFinder.FindAttrPatchFiles("price", INVALID_SCHEMA_OP_ID, patchInfos);

    ASSERT_EQ((size_t)3, patchInfos.size());
    CheckAttrPatchInfos(0, "1,0/segment_2_level_0/attribute/price/2_0.patch",
                        patchInfos);
    CheckAttrPatchInfos(1, "2,0/segment_3_level_0/attribute/price/3_2.patch",
                        patchInfos);
    CheckAttrPatchInfos(3, "4,2/segment_2_level_0/attribute/price/2_1.patch;"
                        "5,2/segment_5_level_0/attribute/price/5_1.patch",
                        patchInfos);
}

void MultiPartPatchFinderTest::TestFindAttrPatchFilesWithException()
{
    {
        //test src segmentid not exist
        MultiPartSegmentDirectoryPtr multiSegDir = 
            CreateMultiPartSegmentDirectory("0,0,0;2,0,0;3,0,0##1,0,0;2,0,0;5,0,0");
        FileSystemWrapper::AtomicStore(
                GET_TEST_DATA_PATH() + "0/segment_2_level_0/attribute/price/1_0.patch", "");
        MultiPartPatchFinder patchFinder;
        patchFinder.Init(multiSegDir);
        
        AttrPatchInfos patchInfos;
        ASSERT_THROW(patchFinder.FindAttrPatchFiles("price", INVALID_SCHEMA_OP_ID, patchInfos), 
                     misc::IndexCollapsedException);
    }
    
    {
        //test dest segmentid not exist
        MultiPartSegmentDirectoryPtr multiSegDir = 
            CreateMultiPartSegmentDirectory("0,0,0;2,0,0;3,0,0##1,0,0;2,0,0;5,0,0");
        FileSystemWrapper::AtomicStore(
                GET_TEST_DATA_PATH() + "0/segment_2_level_0/attribute/price/2_1.patch", "");
        MultiPartPatchFinder patchFinder;
        patchFinder.Init(multiSegDir);
        
        AttrPatchInfos patchInfos;
        ASSERT_THROW(patchFinder.FindAttrPatchFiles("price", INVALID_SCHEMA_OP_ID, patchInfos), 
                     misc::IndexCollapsedException);
    }
    
}

void MultiPartPatchFinderTest::TestFindAttrPatchFilesInSegment()
{
    MultiPartSegmentDirectoryPtr multiSegDir = 
        CreateMultiPartSegmentDirectory("0,0,0;2,0,0;3,0,0##1,0,0;3,0,0;5,0,0");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "0/segment_3_level_0/attribute/price/3_2.patch", "");

    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "0/segment_3_level_0/attribute/price/3_0.patch", "");

    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "2/segment_3_level_0/attribute/price/3_1.patch", "");

    MultiPartPatchFinder patchFinder;
    patchFinder.Init(multiSegDir);

    AttrPatchInfos patchInfos;
    patchFinder.FindAttrPatchFilesInSegment("price", 2, INVALID_SCHEMA_OP_ID, patchInfos);
    ASSERT_EQ((size_t)2, patchInfos.size());
    CheckAttrPatchInfos(0, "2,0/segment_3_level_0/attribute/price/3_0.patch",
                        patchInfos);
    CheckAttrPatchInfos(1, "2,0/segment_3_level_0/attribute/price/3_2.patch",
                        patchInfos);
    
}
void MultiPartPatchFinderTest::TestFindAttrPatchFilesInSegmentWithException()
{
    {
        //test src segmentid not exist
        MultiPartSegmentDirectoryPtr multiSegDir = 
            CreateMultiPartSegmentDirectory("0,0,0;2,0,0;3,0,0##1,0,0;2,0,0;5,0,0");
        FileSystemWrapper::AtomicStore(
                GET_TEST_DATA_PATH() + "0/segment_2_level_0/attribute/price/1_0.patch", "");
        MultiPartPatchFinder patchFinder;
        patchFinder.Init(multiSegDir);
        
        AttrPatchInfos patchInfos;
        ASSERT_THROW(patchFinder.FindAttrPatchFilesInSegment(
                        "price", 1, INVALID_SCHEMA_OP_ID, patchInfos), 
                     misc::IndexCollapsedException);
    }
    
    {
        //test dest segmentid not exist
        MultiPartSegmentDirectoryPtr multiSegDir = 
            CreateMultiPartSegmentDirectory("0,0,0;2,0,0;3,0,0##1,0,0;2,0,0;5,0,0");
        FileSystemWrapper::AtomicStore(
                GET_TEST_DATA_PATH() + "0/segment_2_level_0/attribute/price/2_1.patch", "");
        MultiPartPatchFinder patchFinder;
        patchFinder.Init(multiSegDir);
        
        AttrPatchInfos patchInfos;
        ASSERT_THROW(patchFinder.FindAttrPatchFilesInSegment(
                        "price", 1, INVALID_SCHEMA_OP_ID, patchInfos), 
                     misc::IndexCollapsedException);
    }
}

void MultiPartPatchFinderTest::TestFindAttrPatchFilesForSegment()
{
    MultiPartSegmentDirectoryPtr multiSegDir = 
        CreateMultiPartSegmentDirectory("0,0,0;2,0,0;3,0,0##1,0,0;2,0,0;5,0,0");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "0/segment_2_level_0/attribute/price/2_0.patch", "");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "0/segment_3_level_0/attribute/price/3_2.patch", "");

    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "2/segment_2_level_0/attribute/price/2_1.patch", "");
    FileSystemWrapper::AtomicStore(
            GET_TEST_DATA_PATH() + "2/segment_5_level_0/attribute/price/5_1.patch", "");

    MultiPartPatchFinder patchFinder;
    patchFinder.Init(multiSegDir);

    AttrPatchFileInfoVec patchInfo;
    patchFinder.FindAttrPatchFilesForSegment("price", 0, INVALID_SCHEMA_OP_ID, patchInfo);
    CheckAttrPatchInfoForSegment("1,0/segment_2_level_0/attribute/price/2_0.patch", patchInfo);

    patchFinder.FindAttrPatchFilesForSegment("price", 1, INVALID_SCHEMA_OP_ID, patchInfo);
    CheckAttrPatchInfoForSegment("2,0/segment_3_level_0/attribute/price/3_2.patch", patchInfo);

    patchFinder.FindAttrPatchFilesForSegment("price", 2, INVALID_SCHEMA_OP_ID, patchInfo);
    ASSERT_TRUE(patchInfo.empty());
    patchFinder.FindAttrPatchFilesForSegment("price", 3, INVALID_SCHEMA_OP_ID, patchInfo);
    CheckAttrPatchInfoForSegment(
            "4,2/segment_2_level_0/attribute/price/2_1.patch;"
            "5,2/segment_5_level_0/attribute/price/5_1.patch", patchInfo);

    patchFinder.FindAttrPatchFilesForSegment("price", 4, INVALID_SCHEMA_OP_ID, patchInfo);
    ASSERT_TRUE(patchInfo.empty());
    patchFinder.FindAttrPatchFilesForSegment("price", 5, INVALID_SCHEMA_OP_ID, patchInfo);
    ASSERT_TRUE(patchInfo.empty());
}

//MutiPartitionStr = partitionStr#partitionStr...
//partitionStr = segmentid,docCount,locator;segmentid...
//1,2,3;2,2,3#1,2,3
MultiPartSegmentDirectoryPtr
MultiPartPatchFinderTest::CreateMultiPartSegmentDirectory(
        const string& multiPartSegStrs)
{
    TearDown();
    SetUp();
    StringTokenizer strToken(multiPartSegStrs, "#");
    DirectoryPtr caseRoot = GET_PARTITION_DIRECTORY();

    DirectoryVector partDirs;
    for (size_t i = 0; i < strToken.getNumTokens(); i++)
    {
        DirectoryPtr partDir = 
            caseRoot->MakeDirectory(StringUtil::toString(i));
        partDirs.push_back(partDir);
        PartitionDataMaker::MakePartitionDataFiles(
                i, 0, partDir, strToken[i]);
    }

    MultiPartSegmentDirectoryPtr multiSegDir(new MultiPartSegmentDirectory);
    multiSegDir->Init(partDirs);
    return multiSegDir;
}

void MultiPartPatchFinderTest::CheckDeletionMapPatchInfo(
        segmentid_t virtualSegId, const string& expectPath,
        const DeletePatchInfos& patchInfos)
{
    vector<string> tmpStrs;
    StringUtil::fromString(expectPath, tmpStrs, ",");
    assert(tmpStrs.size() == 2);

    DeletePatchInfos::const_iterator iter = 
        patchInfos.find(virtualSegId);
    ASSERT_TRUE(iter != patchInfos.end());

    PatchFileInfo fileInfo = iter->second;
    segmentid_t srcId;
    StringUtil::fromString(tmpStrs[0], srcId);
    ASSERT_EQ(srcId, fileInfo.srcSegment);

    string actPath = PathUtil::JoinPath(
            fileInfo.patchDirectory->GetPath(), 
            fileInfo.patchFileName);
    ASSERT_EQ(GET_TEST_DATA_PATH() + tmpStrs[1], actPath);
}

void MultiPartPatchFinderTest::CheckAttrPatchInfoForSegment(
        const string& patchInfoStr,
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

        ASSERT_EQ(GET_TEST_DATA_PATH() + expectPatchPath, actualPath);
    }
}

// patchInfoStr:
// srcId0,patchFilePath0;srcId1,patchFilePath1;...
void MultiPartPatchFinderTest::CheckAttrPatchInfos(
        segmentid_t dstSegId,
        const string& patchInfoStr,
        const AttrPatchInfos& patchInfos)
{
    AttrPatchInfos::const_iterator iter = 
        patchInfos.find(dstSegId);
    ASSERT_TRUE(iter != patchInfos.end());

    const AttrPatchFileInfoVec& patchInfo = iter->second;
    CheckAttrPatchInfoForSegment(patchInfoStr, patchInfo);
}


IE_NAMESPACE_END(index_base);

