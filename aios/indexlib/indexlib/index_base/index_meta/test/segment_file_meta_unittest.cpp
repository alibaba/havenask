#include "indexlib/index_base/index_meta/test/segment_file_meta_unittest.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include <autil/StringUtil.h>


using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentFileMetaTest);

SegmentFileMetaTest::SegmentFileMetaTest()
{
}

SegmentFileMetaTest::~SegmentFileMetaTest()
{
}

void SegmentFileMetaTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema.reset(new config::IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(mSchema,
            //Field schema
            "string0:string;string1:string;long1:uint32;", 
            //Primary key index schema
            "pk:primarykey64:string1",
            //Attribute schema
            "long1;string0;string1;",
            //Summary schema
            "string1" );    
}

void SegmentFileMetaTest::CaseTearDown()
{
}

void SegmentFileMetaTest::TestSimpleProcess()
{
    PrepareIndex(false, false);
    auto instanceRootDir = DirectoryCreator::Create(mRootDir);
    Version version;
    VersionLoader::GetVersion(mRootDir, version, INVALID_VERSION);
    auto segIds = version.GetSegmentVector();
    ASSERT_EQ(1, segIds.size());
    auto segDirName = version.GetSegmentDirName(segIds[0]);
    auto segDir = instanceRootDir->GetDirectory(segDirName, true);
    SegmentFileMeta meta;
    ASSERT_TRUE(meta.Load(segDir, false));
    ASSERT_TRUE(meta.IsExist("index"));
    ASSERT_TRUE(meta.IsExist("index/pk"));
    ASSERT_FALSE(meta.IsExist("index/long1"));
    fslib::FileList expectedFileList{"attribute", "counter", "deletionmap", "index", "segment_info", "segment_metrics"};
    fslib::FileList fileList;
    meta.ListFile("", fileList);
    ASSERT_EQ(expectedFileList, fileList);

    fileList.clear();
    meta.ListFile("attribute", fileList, true);
    fslib::FileList expectedFileList2{"long1/", "long1/data", "string0/", "string0/data", "string0/data_info", "string0/offset", "string1/", "string1/data", "string1/data_info", "string1/offset"};
    ASSERT_EQ(expectedFileList2, fileList);
}

void SegmentFileMetaTest::TestPackageFile()
{
    PrepareIndex(false, true);
    auto instanceRootDir = DirectoryCreator::Create(mRootDir);
    Version version;
    VersionLoader::GetVersion(mRootDir, version, INVALID_VERSION);
    auto segIds = version.GetSegmentVector();
    ASSERT_EQ(1, segIds.size());
    auto segDirName = version.GetSegmentDirName(segIds[0]);
    auto segDir = instanceRootDir->GetDirectory(segDirName, true);
    SegmentFileMeta meta;
    ASSERT_TRUE(meta.Load(segDir, false));

    fslib::FileList expectedFileList{"attribute", "counter", "deletionmap", "index", "segment_info", "segment_metrics"};
    fslib::FileList fileList;
    meta.ListFile("", fileList);
    ASSERT_EQ(expectedFileList, fileList);

    fileList.clear();
    meta.ListFile("attribute/long1", fileList);
    fslib::FileList expectedFileList2{"data"};
    ASSERT_EQ(expectedFileList2, fileList);
}

void SegmentFileMetaTest::TestSubSegment()
{
    PrepareIndex(true, true);
    auto instanceRootDir = DirectoryCreator::Create(mRootDir);
    Version version;
    VersionLoader::GetVersion(mRootDir, version, INVALID_VERSION);
    auto segIds = version.GetSegmentVector();
    ASSERT_EQ(1, segIds.size());
    auto segDirName = version.GetSegmentDirName(segIds[0]);
    auto segDir = instanceRootDir->GetDirectory(segDirName, true);
    SegmentFileMeta meta;
    ASSERT_TRUE(meta.Load(segDir, false));

    fslib::FileList expectedFileList{"attribute", "counter", "deletionmap", "index", "segment_info",
            "segment_metrics", "sub_segment"};
    fslib::FileList fileList;
    meta.ListFile("", fileList);
    ASSERT_EQ(expectedFileList, fileList);

    fslib::FileList expectedFileList2{"attribute", "counter", "deletionmap", "index", "segment_info", "segment_metrics"};
    fileList.clear();
    meta.ListFile("sub_segment/", fileList);
    ASSERT_EQ(expectedFileList2, fileList) << StringUtil::toString(fileList);

    ASSERT_TRUE(meta.IsExist("sub_segment/index/sub_pk"));
    ASSERT_TRUE(meta.IsExist("sub_segment/attribute/sub_field2/"));
    ASSERT_TRUE(meta.IsExist("sub_segment/attribute/sub_field2/data"));

    SegmentFileMeta meta2;
    ASSERT_TRUE(meta2.Load(segDir, true));
    fileList.clear();
    meta2.ListFile("index/", fileList);
    fslib::FileList expectedFileList3{"sub_pk"};
    ASSERT_EQ(expectedFileList3, fileList) << StringUtil::toString(fileList);
}

void SegmentFileMetaTest::PrepareIndex(bool hasSub, bool hasPackage)
{
    string docString = "cmd=add,string1=pk1,string0=s1,long1=1;"
                       "cmd=add,string1=pk2,string0=s1,long1=2;"
                       "cmd=add,string1=pk3,string0=s2,long1=3";
    if (hasSub)
    {
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema,
                "sub_field1:uint32;sub_field2:uint32", // field schema
                "sub_pk:primarykey64:sub_field1", // index schema
                "sub_field1;sub_field2", // attribute schema
                "");
        mSchema->SetSubIndexPartitionSchema(subSchema);
        docString = "cmd=add,string1=pk1,string0=s1,long1=1,sub_field1=1,sub_field2=1;"
                    "cmd=add,string1=pk2,string0=s1,long1=2,sub_field1=2,sub_field2=2;"
                    "cmd=add,string1=pk3,string0=s2,long1=3,sub_field1=2,sub_field2=2;";
    }
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    if (hasPackage)
    {
        options.SetEnablePackageFile(true);
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }
    else
    {
        options.SetEnablePackageFile(false);
        ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    }
}


IE_NAMESPACE_END(index_base);

