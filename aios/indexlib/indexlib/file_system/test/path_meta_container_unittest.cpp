#include <autil/StringUtil.h>
#include "indexlib/file_system/test/path_meta_container_unittest.h"

using namespace autil;
using namespace fslib;
using namespace std;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PathMetaContainerTest);

PathMetaContainerTest::PathMetaContainerTest()
{
}

PathMetaContainerTest::~PathMetaContainerTest()
{
}

void PathMetaContainerTest::CaseSetUp()
{
}

void PathMetaContainerTest::CaseTearDown()
{
}

void PathMetaContainerTest::TestSimpleProcess()
{
    string fileInfoStr = "index/:0:1;"
                         "index/indexName/posting:12:10000;"
                         "index/indexName/dictionary:18:10001;"
                         "attribute/:0:10002;"
                         "attribute/attrName/:0:10003;"
                         "attribute/attrName/data:32:10004;";

    vector<FileInfo> fileInfos = MakeFileInfo(fileInfoStr);
    PathMetaContainer pmc;
    ASSERT_TRUE(pmc.AddFileInfoForOneSolidPath("hdfs://indexRoot/segment_0/",
                                               fileInfos.begin(), fileInfos.end()));

    // test match solid path
    ASSERT_TRUE(pmc.MatchSolidPath("hdfs://indexRoot/segment_0/"));
    ASSERT_TRUE(pmc.MatchSolidPath("hdfs://indexRoot/segment_0"));
    ASSERT_FALSE(pmc.MatchSolidPath("hdfs://indexRoot/segment_0_new"));
    ASSERT_TRUE(pmc.MatchSolidPath("hdfs://indexRoot/segment_0/index"));
    ASSERT_TRUE(pmc.MatchSolidPath("hdfs://indexRoot/segment_0/attribute/"));

    // test add single file info
    string diskFile = string(TEST_DATA_PATH"/schema/array_table_schema.json");
    ASSERT_TRUE(pmc.AddFileInfo("hdfs://indexRoot/segment_0/index/new_index", 0, 10004, true));
    ASSERT_TRUE(pmc.AddFileInfo(diskFile, -1, 10006, false));
    ASSERT_TRUE(pmc.AddFileInfo(GET_TEST_DATA_PATH() + "/schema/",
                                -1, 10006, true));
    ASSERT_FALSE(pmc.AddFileInfo("hdfs://indexRoot/segment_0/index/new_index", 0, 10006, false));
    EXPECT_THROW(pmc.AddFileInfo("not_exist_file", -1, 10006, false), NonExistException);

    // test GetFileInfo
    FileInfo fileInfo;
    ASSERT_TRUE(pmc.GetFileInfo("hdfs://indexRoot/segment_0/index/new_index/", fileInfo));
    ASSERT_TRUE(fileInfo.isDirectory());
    ASSERT_EQ(10004, fileInfo.modifyTime);

    ASSERT_TRUE(pmc.IsExist("hdfs://indexRoot/segment_0"));
    ASSERT_TRUE(pmc.IsExist("hdfs://indexRoot/segment_0/"));
    
    pmc.MarkNotExistSolidPath("hdfs://indexRoot/not_exist_segment");
    ASSERT_FALSE(pmc.IsExist("hdfs://indexRoot/not_exist_segment"));
    ASSERT_TRUE(pmc.MatchSolidPath("hdfs://indexRoot/not_exist_segment/"));
    
    ASSERT_TRUE(pmc.IsExist("hdfs://indexRoot/segment_0/index/indexName/posting"));
    ASSERT_FALSE(pmc.IsExist("hdfs://indexRoot/segment_0/index/non_exist/posting"));
    ASSERT_EQ(12, pmc.GetFileLength("hdfs://indexRoot/segment_0/index/indexName/posting"));
    ASSERT_EQ(718, pmc.GetFileLength(diskFile));

    // test listFile
    FileList files;
    ASSERT_TRUE(pmc.ListFile("hdfs://indexRoot/segment_0", files, false));
    ASSERT_THAT(files, UnorderedElementsAre("index", "attribute"));

    ASSERT_TRUE(pmc.ListFile("hdfs://indexRoot/segment_0", files, true));
    ASSERT_THAT(files, UnorderedElementsAre(
                    "index/",
                    "index/indexName/posting",
                    "index/indexName/dictionary",
                    "index/new_index/",
                    "attribute/",
                    "attribute/attrName/",
                    "attribute/attrName/data"));

    // test remove file
    ASSERT_TRUE(pmc.RemoveFile("hdfs://indexRoot/segment_0/index/indexName/posting"));
    ASSERT_FALSE(pmc.IsExist("hdfs://indexRoot/segment_0/index/indexName/posting"));
}

void PathMetaContainerTest::TestRemoveDirectory()
{
    string fileInfoStr = "index/:0:1;"
                         "index/indexName/posting:12:10000;"
                         "index/indexName/dictionary:18:10001;"
                         "attribute/:0:10002;"
                         "attribute/attrName/:0:10003;"
                         "attribute/attrName/data:32:10004;";

    vector<FileInfo> fileInfos = MakeFileInfo(fileInfoStr);
    PathMetaContainer pmc;
    ASSERT_TRUE(pmc.AddFileInfoForOneSolidPath("hdfs://indexRoot/segment_0/",
                                               fileInfos.begin(), fileInfos.end()));
    // test remove directory
    ASSERT_TRUE(pmc.RemoveDirectory("hdfs://indexRoot/segment_0/index"));

    FileList files;
    ASSERT_TRUE(pmc.ListFile("hdfs://indexRoot/segment_0", files, true));
    ASSERT_THAT(files, UnorderedElementsAre(
                    "attribute/",
                    "attribute/attrName/",
                    "attribute/attrName/data"));
    ASSERT_FALSE(pmc.RemoveDirectory("hdfs://indexRoot/segment_0/attribute/attrName/data"));

    // test remove directory which is solid
    ASSERT_TRUE(pmc.RemoveDirectory("hdfs://indexRoot/segment_0"));
    ASSERT_TRUE(pmc.ListFile("hdfs://indexRoot/segment_0", files, true));
    ASSERT_TRUE(files.empty());    

    // test remove directory which is include solid path
    ASSERT_TRUE(pmc.AddFileInfoForOneSolidPath("hdfs://indexRoot/segment_0/", fileInfos.begin(), fileInfos.end()));
    ASSERT_TRUE(pmc.AddFileInfoForOneSolidPath("hdfs://indexRoot/segment_1/", fileInfos.begin(), fileInfos.end()));
    ASSERT_TRUE(pmc.ListFile("hdfs://indexRoot/segment_0", files, true));
    ASSERT_FALSE(files.empty());
    ASSERT_TRUE(pmc.MatchSolidPath("hdfs://indexRoot/segment_0"));
    ASSERT_TRUE(pmc.MatchSolidPath("hdfs://indexRoot/segment_1"));

    ASSERT_TRUE(pmc.RemoveDirectory("hdfs://indexRoot/"));
    ASSERT_FALSE(pmc.MatchSolidPath("hdfs://indexRoot/segment_0"));
    ASSERT_FALSE(pmc.MatchSolidPath("hdfs://indexRoot/segment_1"));
    ASSERT_TRUE(pmc.ListFile("hdfs://indexRoot", files, true));
    ASSERT_TRUE(files.empty());
}

vector<FileInfo> PathMetaContainerTest::MakeFileInfo(const string& fileInfoStr)
{
    vector<FileInfo> fileInfos;
    vector<vector<string> > fileStrs;
    StringUtil::fromString(fileInfoStr, fileStrs, ":", ";");

    for (size_t i = 0; i < fileStrs.size(); i++)
    {
        assert(fileStrs[i].size() == 3);
        FileInfo info(fileStrs[i][0],
                      StringUtil::numberFromString<int64_t>(fileStrs[i][1]),
                      StringUtil::numberFromString<uint64_t>(fileStrs[i][2]));
        fileInfos.push_back(info);
    }
    return fileInfos;
}

IE_NAMESPACE_END(file_system);

