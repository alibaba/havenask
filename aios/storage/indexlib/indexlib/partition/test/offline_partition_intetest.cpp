#include "indexlib/partition/test/offline_partition_intetest.h"

#include <time.h>

#include "autil/ConstString.h"
#include "autil/StringUtil.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/source/source_reader.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using autil::StringView;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OfflinePartitionInteTest);

OfflinePartitionInteTest::OfflinePartitionInteTest() {}

OfflinePartitionInteTest::~OfflinePartitionInteTest() {}

void OfflinePartitionInteTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GET_PARAM_VALUE(2));
    mOptions = options;
    mOptions.SetIsOnline(false);
    mOptions.GetBuildConfig().speedUpPrimaryKeyReader = std::get<0>(GET_CASE_PARAM());
    mOptions.GetBuildConfig().enablePackageFile = std::get<1>(GET_CASE_PARAM());
}

void OfflinePartitionInteTest::CaseTearDown() {}

void OfflinePartitionInteTest::TestAddDocument()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index1:string:string1;index2:string:string2";
    string attribute = "string1;price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5;"
                       "cmd=add,string1=hello,price=6;";

    mOptions.GetBuildConfig().maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index1:hello", "docid=0;docid=1;docid=2"));
    Version version;
    string versionFile = util::PathUtil::JoinPath(mRootDir, "version.0");
    version.Load(versionFile);
    ASSERT_EQ((size_t)3, version.GetSegmentCount());
}

void OfflinePartitionInteTest::TestIgnoreDocument()
{
    string field = "pkstr:string;long1:uint64;";
    string index = "pk:primarykey64:pkstr;";
    string attribute = "pkstr;long1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->SetInsertOrIgnore(true);

    string docString = "cmd=add,pkstr=hello,long1=4;"
                       "cmd=add,pkstr=hello,long1=5;"
                       "cmd=add,pkstr=hello,long1=6;";
    mOptions.GetBuildConfig().maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", "docid=0,pkstr=hello,long1=4;"));
}

void OfflinePartitionInteTest::TestMergeErrorWithBuildFileLost()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index1:string:string1;index2:string:string2";
    string attribute = "string1;price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5;"
                       "cmd=add,string1=hello,price=6;";

    mOptions.GetBuildConfig().maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_FULL, docString, "", ""));

    ParallelBuildInfo info(1, 0, 0);

    {
        string deleteStr = mRootDir;
        deleteStr = deleteStr.substr(0, deleteStr.length() - 1);
        FslibWrapper::RenameE(deleteStr, deleteStr + "__temp__");
        ASSERT_ANY_THROW(DoStepMerge("BEGIN_MERGE", info, true));
        FslibWrapper::DeleteFileE(deleteStr, DeleteOption::NoFence(true));
        FslibWrapper::RenameE(deleteStr + "__temp__", deleteStr);
    }
    {
        string deleteStr = util::PathUtil::JoinPath(mRootDir, "segment_1_level_0");
        FslibWrapper::RenameE(deleteStr, deleteStr + "__temp__");

        ASSERT_ANY_THROW(DoStepMerge("BEGIN_MERGE", info, true));
        FslibWrapper::DeleteFileE(deleteStr, DeleteOption::NoFence(true));
        FslibWrapper::RenameE(deleteStr + "__temp__", deleteStr);
    }
    {
        string fileName = "segment_1_level_0/index";
        if (mOptions.GetBuildConfig().enablePackageFile) {
            fileName = "segment_1_level_0/package_file.__data__0";
        }
        string deleteStr = util::PathUtil::JoinPath(mRootDir, fileName);
        FslibWrapper::RenameE(deleteStr, deleteStr + "__temp__");

        // ASSERT_ANY_THROW(DoStepMerge("BEGIN_MERGE", info, true));
        // todo delete index file throw exception
        FslibWrapper::DeleteFileE(deleteStr, DeleteOption::NoFence(true));
        FslibWrapper::RenameE(deleteStr + "__temp__", deleteStr);
    }
    ASSERT_NO_THROW(DoStepMerge("BEGIN_MERGE", info, true));
    /*
    {
        string fileName = "segment_1_level_0/index";
        if (mOptions.GetBuildConfig().enablePackageFile)
        {
            fileName = "segment_1_level_0/package_file.__data__0";
        }
        string deleteStr = util::PathUtil::JoinPath(mRootDir, fileName);
        FslibWrapper::RenameE(deleteStr, deleteStr + "__temp__");
        //ASSERT_ANY_THROW(DoStepMerge("DO_MERGE", info, true));
        //todo delete index file throw exception
        FslibWrapper::RenameE(deleteStr + "__temp__", deleteStr);
    }
    ASSERT_NO_THROW(DoStepMerge("DO_MERGE", info, true));
    */
}

void OfflinePartitionInteTest::TestMergeErrorWithMergeFileLost()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index1:string:string1;index2:string:string2";
    string attribute = "string1;price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5;"
                       "cmd=add,string1=hello,price=6;";

    mOptions.GetBuildConfig().maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_FULL, docString, "", ""));

    ParallelBuildInfo info(1, 0, 0);
    ASSERT_NO_THROW(DoStepMerge("BEGIN_MERGE", info, true));
    {
        string fileName = "segment_1_level_0";
        string deleteStr = util::PathUtil::JoinPath(mRootDir, fileName);
        FslibWrapper::RenameE(deleteStr, deleteStr + "__temp__");
        ASSERT_ANY_THROW(DoStepMerge("DO_MERGE", info, true));
        FslibWrapper::RenameE(deleteStr + "__temp__", deleteStr);
    }
    {
        string fileName = "segment_1_level_0/index";
        if (mOptions.GetBuildConfig().enablePackageFile) {
            fileName = "segment_1_level_0/package_file.__data__0";
        }
        string deleteStr = util::PathUtil::JoinPath(mRootDir, fileName);
        FslibWrapper::RenameE(deleteStr, deleteStr + "__temp__");
        // ASSERT_ANY_THROW(DoStepMerge("DO_MERGE", info, true));
        // todo delete index file throw exception
        FslibWrapper::RenameE(deleteStr + "__temp__", deleteStr);
    }

    unique_ptr<merger::IndexPartitionMerger> merger(
        (merger::IndexPartitionMerger*)merger::PartitionMergerCreator::TEST_CreateIncParallelPartitionMerger(
            mRootDir, info, mOptions, NULL, ""));
    merger::MergeMetaPtr meta = merger->CreateMergeMeta(true, 1, 0);
    merger->DoMerge(true, meta, 0);
    {
        string branchRoot =
            util::PathUtil::JoinPath(mRootDir, "instance_0/", merger->TEST_GetBranchFs()->GetBranchName());
        string fileName = "segment_3_level_0";
        string deleteStr = util::PathUtil::JoinPath(branchRoot, fileName);
        FslibWrapper::RenameE(deleteStr, deleteStr + "__temp__");
        // ASSERT_ANY_THROW(DoStepMerge("END_MERGE", info, true));
        // todo delete segment file throw exception
        FslibWrapper::RenameE(deleteStr + "__temp__", deleteStr);
    }
    {
        string branchRoot =
            util::PathUtil::JoinPath(mRootDir, "instance_0/", merger->TEST_GetBranchFs()->GetBranchName());
        string fileName = "segment_3_level_0/index";
        string deleteStr = util::PathUtil::JoinPath(branchRoot, fileName);
        FslibWrapper::RenameE(deleteStr, deleteStr + "__temp__");
        // ASSERT_ANY_THROW(DoStepMerge("END_MERGE", info, true));
        // todo delete index file throw exception
        FslibWrapper::RenameE(deleteStr + "__temp__", deleteStr);
    }
    ASSERT_NO_THROW(DoStepMerge("END_MERGE", info, true));
}

void OfflinePartitionInteTest::TestDelDocument()
{
    string docString = "cmd=add,string1=hello1,string2=hello,price=4;"
                       "cmd=add,string1=hello2,string2=hello,price=5;"
                       "cmd=add,string1=hello3,string2=hello,price=6;"
                       "cmd=delete,string1=hello3;";

    mOptions.GetBuildConfig().maxDocCount = 2;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index2:hello", "docid=0;docid=1"));

    string delDocString = "cmd=delete,string1=hello2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, delDocString, "index2:hello", "docid=0"));
}

void OfflinePartitionInteTest::TestDedupAddDocument()
{
    string docString = "cmd=add,string1=hello1,string2=hello,price=4;"
                       "cmd=add,string1=hello2,string2=hello,price=5;"
                       "cmd=add,string1=hello3,string2=hello,price=6;"
                       "cmd=add,string1=hello1,string2=hello,price=7;";

    mOptions.GetBuildConfig().maxDocCount = 2;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index2:hello", "docid=1;docid=2;docid=3"));

    string incDocString = "cmd=add,string1=hello3,string2=hello,price=8;"
                          "cmd=add,string1=hello3,string2=hello,price=9;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "index2:hello", "docid=1;docid=3;docid=5"));
}

void OfflinePartitionInteTest::TestUpdateDocument()
{
    string docString = "cmd=add,string1=hello1,string2=hello,price=4;"
                       "cmd=update_field,string1=hello1,price=5;"
                       "cmd=add,string1=hello2,string2=hello,price=6;"
                       "cmd=add,string1=hello3,string2=hello,price=7;"
                       "cmd=update_field,string1=hello2,price=8;";

    mOptions.GetBuildConfig().maxDocCount = 2;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index2:hello",
                                    "docid=0,price=5;docid=1,price=8;docid=2,price=7"));

    string incDocString = "cmd=update_field,string1=hello3,price=9;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "index2:hello",
                                    "docid=0,price=5;docid=1,price=8;docid=2,price=9"));
}

void OfflinePartitionInteTest::TestAddDocWithoutPkWhenSchemaHasPkIndex()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docString = "cmd=add,string2=hello,price=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index2:hello", ""));
}

void OfflinePartitionInteTest::TestBuildingSegmentDedup()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docString = "cmd=add,string1=hello1,string2=hello,price=4;"
                       "cmd=add,string1=hello1,string2=hello,price=5;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index2:hello", "docid=1,price=5"));

    string incDocString = "cmd=add,string1=hello3,string2=hello,price=8;"
                          "cmd=add,string1=hello3,string2=hello,price=9;";
    INDEXLIB_TEST_TRUE(
        psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "index2:hello", "docid=1,price=5;docid=3,price=9"));
}

void OfflinePartitionInteTest::TestBuiltSegmentsDedup()
{
    IndexConfigPtr pkConfig = mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    PrimaryKeyIndexConfigPtr config = DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, pkConfig);
    config->SetPrimaryKeyIndexType(pk_sort_array);

    PartitionStateMachine psm;
    mOptions.GetBuildConfig().maxDocCount = 1;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docString = "cmd=add,string1=hello1,string2=hello,price=4;"
                       "cmd=add,string1=hello1,string2=hello,price=5;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "index2:hello", "docid=1,price=5"));

    string incDocString = "cmd=add,string1=hello1,string2=hello,price=8;"
                          "cmd=add,string1=hello1,string2=hello,price=9;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "index2:hello", "docid=3,price=9"));
}

void OfflinePartitionInteTest::TestPartitionMetrics()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello2,price=4;"
                       "cmd=update_field,string1=hello,price=5;"
                       "cmd=add,string1=hello3,price=4;";

    mOptions.GetBuildConfig().maxDocCount = 1;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    HasMetricsValue(psm, "indexlib/build/buildMemoryUse", kmonitor::GAUGE, "byte");
    HasMetricsValue(psm, "indexlib/build/segmentWriterMemoryUse", kmonitor::GAUGE, "byte");
    HasMetricsValue(psm, "indexlib/build/modifierMemoryUse", kmonitor::GAUGE, "byte");
}

void OfflinePartitionInteTest::TestOpenWithSpecificVersion()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello2,price=4;"
                       "cmd=update_field,string1=hello,price=5;"
                       "cmd=add,string1=hello3,price=4;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "", ""));

    OfflinePartition offlinePart;
    offlinePart.Open(GET_TEMP_DATA_PATH(), "", mSchema, mOptions, 0);

    IndexPartitionReaderPtr reader = offlinePart.GetReader();
    Version version = reader->GetVersion();
    ASSERT_EQ((versionid_t)0, version.GetVersionId());
    offlinePart.Close();
}

void OfflinePartitionInteTest::TestRecoverFromLegacySegmentDirName()
{
    string indexDir = GET_PRIVATE_TEST_DATA_PATH() + "/compatible_index_for_segment_directory_name";
    string rootDir = GET_TEMP_DATA_PATH() + "/part_dir";
    fslib::fs::FileSystem::copy(indexDir, rootDir);
    auto rootDirectory = GET_CHECK_DIRECTORY()->GetDirectory("part_dir", false);
    ASSERT_NE(rootDirectory, nullptr);
    IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GET_PARAM_VALUE(2));
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadAndRewritePartitionSchema(rootDirectory, options);

    string versionPath = util::PathUtil::JoinPath(rootDir, "version.1");
    string segInfoPath = util::PathUtil::JoinPath(rootDir, "segment_2/segment_info");
    fslib::fs::FileSystem::remove(versionPath);
    fslib::fs::FileSystem::remove(segInfoPath);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "cmd=add,pk=3,string1=hello,price=3;", "index:hello",
                             "price=0;price=1;price=3"));
}

void OfflinePartitionInteTest::TestRecoverFromLegacySegmentDirNameWithNoVersion()
{
    string indexDir = GET_PRIVATE_TEST_DATA_PATH() + "/compatible_index_for_segment_directory_name";
    string rootDir = GET_TEMP_DATA_PATH() + "/part_dir";
    fslib::fs::FileSystem::copy(indexDir, rootDir);
    auto rootDirectory = GET_CHECK_DIRECTORY()->GetDirectory("part_dir", false);
    ASSERT_NE(rootDirectory, nullptr);

    IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GET_PARAM_VALUE(2));
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadAndRewritePartitionSchema(rootDirectory, options);

    string versionPath0 = util::PathUtil::JoinPath(rootDir, "version.0");
    string versionPath1 = util::PathUtil::JoinPath(rootDir, "version.1");
    string segInfoPath = util::PathUtil::JoinPath(rootDir, "segment_2/segment_info");
    fslib::fs::FileSystem::remove(versionPath0);
    fslib::fs::FileSystem::remove(versionPath1);
    fslib::fs::FileSystem::remove(segInfoPath);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "cmd=add,pk=3,string1=hello,price=3;", "index:hello", "price=3"));
}

void OfflinePartitionInteTest::TestRecoverWithNoVersion()
{
    mOptions.GetBuildConfig().maxDocCount = 1;
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        string docString = "cmd=add,string1=hello1,string2=hello,price=4;"
                           "cmd=add,string1=hello2,string2=hello,price=5;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }
    string versionPath0 = util::PathUtil::JoinPath(mRootDir, "version.0");
    string entryTablePath = util::PathUtil::JoinPath(mRootDir, "entry_table.0");
    string segInfoPath = util::PathUtil::JoinPath(mRootDir, "segment_1_level_0/segment_info");
    fslib::fs::FileSystem::remove(versionPath0);
    fslib::fs::FileSystem::remove(entryTablePath);
    fslib::fs::FileSystem::remove(segInfoPath);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,string1=hello3,string2=hello,price=6;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "index2:hello", "price=4;price=6"));
}

void OfflinePartitionInteTest::TestNullFieldWithUpdate()
{
    PartitionStateMachine psm;
    string field = "string1:string;string2:string;price:uint32::::-1:true";
    string index = "pk:primarykey64:string1;index2:string:string2";
    string attribute = "string1;price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mOptions.GetOnlineConfig().loadPatchThreadNum = 2;
    ASSERT_TRUE(psm.Init(schema, mOptions, mRootDir));
    string docString = "cmd=add,string1=hello1,string2=hello,price=__NULL__;"
                       "cmd=add,string1=hello2,string2=hello,price=4;"
                       "cmd=add,string1=hello3,string2=hello,price=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    string incDocString = "cmd=update_field,string1=hello3,string2=hello,price=__NULL__;"
                          "cmd=update_field,string1=hello1,string2=hello,price=6;"
                          "cmd=add,string1=hello4,string2=hello,price=9;"
                          "cmd=add,string1=hello4,string2=hello,price=__NULL__;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
    autil::mem_pool::Pool pool;
    auto& pkReader = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    const AttributeReaderPtr attrReader = psm.GetIndexPartition()->GetReader()->GetAttributeReader("price");
    string priceValue;
    docid_t docId = pkReader->Lookup("hello3");
    attrReader->Read(docId, priceValue);
    ASSERT_EQ("__NULL__", priceValue);
    docId = pkReader->Lookup("hello1");
    attrReader->Read(docId, priceValue);
    ASSERT_NE("__NULL__", priceValue);
    ASSERT_EQ("6", priceValue);
    docId = pkReader->Lookup("hello4");
    attrReader->Read(docId, priceValue);
    ASSERT_EQ("__NULL__", priceValue);

    string incDocString2 = "cmd=update_field,string1=hello3,string2=hello,price=7;"
                           "cmd=add,string1=hello4,string2=hello,price=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString2, "", ""));
    auto& pkReader2 = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    const AttributeReaderPtr attrReader2 = psm.GetIndexPartition()->GetReader()->GetAttributeReader("price");
    docId = pkReader2->Lookup("hello3");
    attrReader2->Read(docId, priceValue);
    ASSERT_EQ("7", priceValue);
    docId = pkReader2->Lookup("hello4");
    attrReader2->Read(docId, priceValue);
    ASSERT_EQ("8", priceValue);

    string rtDocString1 = "cmd=update_field,string1=hello3,string2=hello,price=__NULL__;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString1, "", ""));
    auto& pkReader3 = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    const AttributeReaderPtr attrReader3 = psm.GetIndexPartition()->GetReader()->GetAttributeReader("price");
    docId = pkReader3->Lookup("hello3");
    attrReader3->Read(docId, priceValue);
    ASSERT_EQ("__NULL__", priceValue);

    string rtDocString2 = "cmd=update_field,string1=hello3,string2=hello,price=100;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "", ""));
    auto& pkReader4 = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    docId = pkReader4->Lookup("hello3");
    const AttributeReaderPtr attrReader4 = psm.GetIndexPartition()->GetReader()->GetAttributeReader("price");
    attrReader4->Read(docId, priceValue);
    ASSERT_EQ("100", priceValue);

    string incDocString3 = "cmd=update_field,string1=hello1,string2=hello,price=__NULL__;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString3, "", ""));
    auto& pkReader5 = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    const AttributeReaderPtr attrReader5 = psm.GetIndexPartition()->GetReader()->GetAttributeReader("price");
    docId = pkReader5->Lookup("hello1");
    attrReader5->Read(docId, priceValue);
    ASSERT_EQ("__NULL__", priceValue);

    // test for multi update
    mOptions.GetBuildConfig().maxDocCount = 2;
    string incDocString4 = "cmd=update_field,string1=hello1,string2=hello,price=3;"
                           "cmd=update_field,string1=hello1,string2=hello,price=__NULL__;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString4, "pk:hello1", "price=__NULL__"));

    string incDocString5 = "cmd=update_field,string1=hello1,string2=hello,price=3;"
                           "cmd=update_field,string1=hello1,string2=hello,price=__NULL__;"
                           "cmd=update_field,string1=hello1,string2=hello,price=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString5, "pk:hello1", "price=4"));
}

void OfflinePartitionInteTest::TestNullFieldSortWithUpdate()
{
    PartitionStateMachine psm;
    string field = "string1:string;string2:string;price:uint32::::-1:true";
    string index = "pk:primarykey64:string1;index2:string:string2";
    string attribute = "string1;price";

    PartitionMeta partitionMeta;
    partitionMeta.AddSortDescription("price", indexlibv2::config::sp_asc);
    partitionMeta.Store(mRootDir, FenceContext::NoFence());

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mOptions.GetOnlineConfig().loadPatchThreadNum = 2;
    ASSERT_TRUE(psm.Init(schema, mOptions, mRootDir));
    string docString = "cmd=add,string1=hello1,string2=hello,price=__NULL__;"
                       "cmd=add,string1=hello2,string2=hello,price=4;"
                       "cmd=add,string1=hello3,string2=hello,price=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    string incDocString = "cmd=update_field,string1=hello3,string2=hello,price=__NULL__;"
                          "cmd=update_field,string1=hello1,string2=hello,price=6;"
                          "cmd=add,string1=hello4,string2=hello,price=__NULL__;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
    autil::mem_pool::Pool pool;
    auto& pkReader = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    const AttributeReaderPtr attrReader = psm.GetIndexPartition()->GetReader()->GetAttributeReader("price");
    string priceValue;
    docid_t docId = pkReader->Lookup("hello3");
    attrReader->Read(docId, priceValue);
    ASSERT_EQ("__NULL__", priceValue);
    docId = pkReader->Lookup("hello1");
    attrReader->Read(docId, priceValue);
    ASSERT_EQ("6", priceValue);
    docId = pkReader->Lookup("hello4");
    attrReader->Read(docId, priceValue);
    ASSERT_EQ("__NULL__", priceValue);

    string incDocString2 = "cmd=update_field,string1=hello3,string2=hello,price=7;"
                           "cmd=add,string1=hello4,string2=hello,price=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString2, "", ""));
    auto& pkReader2 = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    const AttributeReaderPtr attrReader2 = psm.GetIndexPartition()->GetReader()->GetAttributeReader("price");
    docId = pkReader2->Lookup("hello3");
    attrReader2->Read(docId, priceValue);
    ASSERT_EQ("7", priceValue);
    docId = pkReader2->Lookup("hello4");
    attrReader2->Read(docId, priceValue);
    ASSERT_EQ("8", priceValue);

    string rtDocString1 = "cmd=update_field,string1=hello3,string2=hello,price=__NULL__;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString1, "", ""));
    auto& pkReader3 = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    const AttributeReaderPtr attrReader3 = psm.GetIndexPartition()->GetReader()->GetAttributeReader("price");
    docId = pkReader3->Lookup("hello3");
    attrReader3->Read(docId, priceValue);
    ASSERT_EQ("__NULL__", priceValue);

    string rtDocString2 = "cmd=update_field,string1=hello3,string2=hello,price=100;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "", ""));
    auto& pkReader4 = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    docId = pkReader4->Lookup("hello3");
    const AttributeReaderPtr attrReader4 = psm.GetIndexPartition()->GetReader()->GetAttributeReader("price");
    attrReader4->Read(docId, priceValue);
    ASSERT_EQ("100", priceValue);

    string incDocString3 = "cmd=update_field,string1=hello1,string2=hello,price=__NULL__;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString3, "", ""));
    auto& pkReader5 = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    const AttributeReaderPtr attrReader5 = psm.GetIndexPartition()->GetReader()->GetAttributeReader("price");
    docId = pkReader5->Lookup("hello1");
    attrReader5->Read(docId, priceValue);
    ASSERT_EQ("__NULL__", priceValue);
}

void OfflinePartitionInteTest::TestNullField()
{
    string field = "string1:string;string2:string;price:uint32::::-1:true";
    string index = "index1:string:string1;index2:string:string2";
    string attribute = "string1;price";

    // TODO test sort merge
    // PartitionMeta partitionMeta;
    // partitionMeta.AddSortDescription("price", indexlibv2::config::sp_asc);
    // partitionMeta.Store(mRootDir);

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    srand((unsigned)time(NULL));
    string outputSegNum = autil::StringUtil::toString(rand() % 5 + 1);
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"" + outputSegNum + "\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    stringstream docString;
    int64_t docCount = rand() % 2000 + 1;
    std::cout << "run TestNullField with docCount " << docCount << ", outputSeg " << outputSegNum << endl;
    set<int64_t> nullDocs;
    for (int64_t i = 0; i < docCount; i++) {
        bool isNull = (rand() % 2 == 0);
        docString << "cmd=add,string1=hello" << i << ",price=";
        if (isNull) {
            nullDocs.insert(i);
            docString << "__NULL__;";
        } else {
            docString << i << ";";
        }
    }

    mOptions.GetBuildConfig().maxDocCount = docCount / 3;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString.str(), "", ""));
    // check null field
    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    autil::mem_pool::Pool pool;
    const AttributeReaderPtr attrReader = indexPart->GetReader()->GetAttributeReader("price");
    for (int64_t i = 0; i < docCount; i++) {
        string priceValue;
        attrReader->Read(i, priceValue);
        if (nullDocs.find(i) == nullDocs.end()) {
            ASSERT_EQ(to_string(i), priceValue);
        } else {
            ASSERT_EQ("__NULL__", priceValue);
        }
    }
}

void OfflinePartitionInteTest::HasMetricsValue(const PartitionStateMachine& psm, const string& name,
                                               kmonitor::MetricType type, const string& unit)
{
    util::MetricProviderPtr provider = psm.GetMetricsProvider();
    util::MetricPtr metric = provider->DeclareMetric(name, type);
    ASSERT_TRUE(metric != nullptr);
    ASSERT_NE(std::numeric_limits<double>::min(), metric->TEST_GetValue());
}

void OfflinePartitionInteTest::DoStepMerge(const string& step, const ParallelBuildInfo& info, bool optimize)
{
    unique_ptr<merger::IndexPartitionMerger> merger(
        (merger::IndexPartitionMerger*)merger::PartitionMergerCreator::TEST_CreateIncParallelPartitionMerger(
            mRootDir, info, mOptions, NULL, ""));
    merger::MergeMetaPtr meta = merger->CreateMergeMeta(optimize, 1, 0);
    if (step == "BEGIN_MERGE") {
        merger->PrepareMerge(0);
        return;
    }
    if (step == "DO_MERGE") {
        merger->DoMerge(optimize, meta, 0);
        return;
    }
    if (step == "END_MERGE") {
        merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
        return;
    }
    ASSERT_TRUE(false);
}

void OfflinePartitionInteTest::TestFileCompressWithTemperatureLayer() { InnerTestFileCompressWithTemperatureLayer(""); }

void OfflinePartitionInteTest::TestExcludedFileCompressWithTemperatureLayer()
{
    InnerTestFileCompressWithTemperatureLayer(".*/(posting|offset)$");
    InnerTestFileCompressWithTemperatureLayer(".*/(dictionary|data)$");
}

void OfflinePartitionInteTest::InnerTestFileCompressWithTemperatureLayer(const string& excludePattern)
{
    TearDown();
    SetUp();

    string field = "pk:string;string1:string;string2:text;price:uint32";
    string index = "pk:primarykey64:pk;index1:string:string1;index2:pack:string2";
    string attribute = "string1;price";
    string summary = "pk";
    string source = "pk:string1#price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, summary, "", source);
    string temperatureLayerStr = "WARM#type=Range;field_name=price;value=[10,99];value_type=UINT64|"
                                 "COLD#type=Range;field_name=price;value=[100,1000];value_type=UINT64";
    SchemaMaker::EnableTemperatureLayer(schema, temperatureLayerStr, "HOT");

    string compressorStr = "cold_compressor:zstd;"
                           "warm_compressor:lz4;"
                           "temperature_compressor:null:WARM#warm_compressor|COLD#cold_compressor";
    string indexCompressStr = "index1:temperature_compressor;index2:temperature_compressor";
    string attrCompressStr = "string1:temperature_compressor;price:temperature_compressor";
    string summaryCompressStr = "0:temperature_compressor";
    string sourceCompressStr = "0:temperature_compressor;1:temperature_compressor";
    schema = SchemaMaker::EnableFileCompressSchema(schema, compressorStr, indexCompressStr, attrCompressStr,
                                                   summaryCompressStr, sourceCompressStr);
    auto fileCompressSchema = schema->GetFileCompressSchema();
    fileCompressSchema->GetFileCompressConfig("cold_compressor")->TEST_SetParameter("enable_hint_data", "true");
    if (!excludePattern.empty()) {
        fileCompressSchema->GetFileCompressConfig("cold_compressor")->TEST_SetExcludePattern(excludePattern);
        fileCompressSchema->GetFileCompressConfig("warm_compressor")->TEST_SetExcludePattern(excludePattern);
    }

    string docString = "cmd=add,pk=1,string1=hello,price=4,string2=doc;"    // hot
                       "cmd=add,pk=2,string1=hello,price=5,string2=cat;"    // hot
                       "cmd=add,pk=3,string1=hello,price=6,string2=cat;"    // hot
                       "cmd=add,pk=4,string1=hello,price=10;"               // warm
                       "cmd=add,pk=5,string1=hello,price=12,string2=abc;"   // warm
                       "cmd=add,pk=6,string1=hello,price=200,string2=dog;"  // cold
                       "cmd=add,pk=7,string1=hello,price=300,string2=dog;"  // cold
                       "cmd=add,pk=8,string1=hello,price=400,string2=dog;"; // cold

    IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GET_PARAM_VALUE(2));
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().speedUpPrimaryKeyReader = std::get<0>(GET_CASE_PARAM());
    options.GetBuildConfig().enablePackageFile = std::get<1>(GET_CASE_PARAM());

    std::string splitConfigStr = "{\"class_name\":\"temperature\",\"parameters\":{}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), splitConfigStr);

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "index1:hello",
                                    "docid=0,price=4,string1=hello;"
                                    "docid=1,price=5,string1=hello;"
                                    "docid=2,price=6,string1=hello;"
                                    "docid=3,price=10,string1=hello;"
                                    "docid=4,price=12,string1=hello;"
                                    "docid=5,price=200,string1=hello;"
                                    "docid=6,price=300,string1=hello;"
                                    "docid=7,price=400,string1=hello"));

    auto CheckFileCompressInfo = [](const file_system::DirectoryPtr& segDir, const string& filePath,
                                    const string& expectCompressType, const string& pattern) {
        auto compressInfo = segDir->GetCompressFileInfo(filePath);
        if (expectCompressType == "null") {
            EXPECT_TRUE(compressInfo.get() == nullptr);
        } else if (RegularExpression::Match(pattern, filePath)) {
            EXPECT_TRUE(compressInfo.get() == nullptr);
        } else {
            EXPECT_TRUE(compressInfo.get() != nullptr) << segDir->DebugString() << filePath;
            EXPECT_EQ(expectCompressType, compressInfo->compressorName);
        }
    };

    auto CheckSegmentCompressInfo = [&](const index_base::PartitionDataPtr& partData, segmentid_t segId,
                                        const string& expectCompressType, const string& pattern) {
        auto segData = partData->GetSegmentData(segId);
        auto segInfo = *segData.GetSegmentInfo();
        DirectoryPtr segDir = segData.GetDirectory();

        uint64_t fingerPrint;
        ASSERT_TRUE(segInfo.GetCompressFingerPrint(fingerPrint));
        ASSERT_EQ(fingerPrint, fileCompressSchema->GetFingerPrint());
        CheckFileCompressInfo(segDir, "index/index1/posting", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "index/index1/dictionary", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "index/index2_section/offset", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "index/index2_section/data", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "attribute/price/data", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "attribute/string1/offset", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "attribute/string1/data", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "summary/data", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "summary/offset", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "source/group_0/data", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "source/group_0/offset", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "source/group_1/data", expectCompressType, pattern);
        CheckFileCompressInfo(segDir, "source/group_1/offset", expectCompressType, pattern);
    };

    // check build version
    ASSERT_EQ(FSEC_OK, psm.GetFileSystem()->MountVersion(mRootDir, 0, "", FSMT_READ_ONLY, nullptr));
    Version buildVersion;
    VersionLoader::GetVersion(psm.GetRootDirectory(), buildVersion, 0);
    auto partData = OnDiskPartitionData::CreateOnDiskPartitionData(psm.GetFileSystem(), buildVersion);
    ASSERT_EQ(4, partData->GetVersion().GetSegmentCount());

    CheckSegmentCompressInfo(partData, 0, "null", excludePattern); // segment 0 is hot (2 hot)
    CheckSegmentCompressInfo(partData, 1, "null", excludePattern); // segment 1 is hot (1 hot : 1 warm)
    CheckSegmentCompressInfo(partData, 2, "lz4", excludePattern);  // segment 2 is warm (1 warm : 1 cold)
    CheckSegmentCompressInfo(partData, 3, "zstd", excludePattern); // segment 4 is cold (2 cold)

    // check latest version : optimized merge split by temperature
    partData = OnDiskPartitionData::CreateOnDiskPartitionData(psm.GetFileSystem());
    ASSERT_EQ(3, partData->GetVersion().GetSegmentCount());
    CheckSegmentCompressInfo(partData, 4, "null", excludePattern); // segment 4 is hot
    CheckSegmentCompressInfo(partData, 5, "lz4", excludePattern);  // segment 5 is warm
    CheckSegmentCompressInfo(partData, 6, "zstd", excludePattern); // segment 6 is cold
}

void OfflinePartitionInteTest::TestUpdateFileCompressSchema()
{
    string field = "pk:string;string1:string;string2:text;price:uint32";
    string index = "pk:primarykey64:pk;index1:string:string1;index2:pack:string2";
    string attribute = "string1;price";
    string summary = "pk";
    string source = "pk:string1#price";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, summary, "", source);
    string temperatureLayerStr = "WARM#type=Range;field_name=price;value=[10,99];value_type=UINT64|"
                                 "COLD#type=Range;field_name=price;value=[100,1000];value_type=UINT64";
    SchemaMaker::EnableTemperatureLayer(schema, temperatureLayerStr, "HOT");

    string compressorStr = "cold_compressor:zstd;"
                           "warm_compressor:lz4;"
                           "temperature_compressor:null:WARM#warm_compressor|COLD#cold_compressor";
    string indexCompressStr = "index1:temperature_compressor;index2:temperature_compressor";
    string attrCompressStr = "string1:temperature_compressor;price:temperature_compressor";
    string summaryCompressStr = "0:temperature_compressor";
    string sourceCompressStr = "0:temperature_compressor;1:temperature_compressor";
    schema = SchemaMaker::EnableFileCompressSchema(schema, compressorStr, indexCompressStr, attrCompressStr,
                                                   summaryCompressStr, sourceCompressStr);

    string docString = "cmd=add,pk=1,string1=hello,price=4,string2=doc;"    // hot
                       "cmd=add,pk=2,string1=hello,price=5,string2=cat;"    // hot
                       "cmd=add,pk=3,string1=hello,price=6,string2=cat;"    // hot
                       "cmd=add,pk=4,string1=hello,price=10;"               // warm
                       "cmd=add,pk=5,string1=hello,price=12,string2=abc;"   // warm
                       "cmd=add,pk=6,string1=hello,price=200,string2=dog;"  // cold
                       "cmd=add,pk=7,string1=hello,price=300,string2=dog;"  // cold
                       "cmd=add,pk=8,string1=hello,price=400,string2=dog;"; // cold
    IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GET_PARAM_VALUE(2));
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().speedUpPrimaryKeyReader = std::get<0>(GET_CASE_PARAM());
    options.GetBuildConfig().enablePackageFile = std::get<1>(GET_CASE_PARAM());
    std::string splitConfigStr = "{\"class_name\":\"temperature\",\"parameters\":{}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), splitConfigStr);

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    auto CheckFileCompressInfo = [](const file_system::DirectoryPtr& segDir, const string& filePath,
                                    const string& expectCompressType) {
        auto compressInfo = segDir->GetCompressFileInfo(filePath);
        if (expectCompressType == "null") {
            EXPECT_TRUE(compressInfo.get() == nullptr);
        } else {
            EXPECT_TRUE(compressInfo.get() != nullptr) << segDir->DebugString() << filePath;
            EXPECT_EQ(expectCompressType, compressInfo->compressorName);
        }
    };

    auto CheckSegmentCompressInfo = [&](const index_base::PartitionDataPtr& partData, segmentid_t segId,
                                        const string& expectCompressType) {
        auto segData = partData->GetSegmentData(segId);
        auto segInfo = *segData.GetSegmentInfo();
        DirectoryPtr segDir = segData.GetDirectory();
        CheckFileCompressInfo(segDir, "index/index1/posting", expectCompressType);
        CheckFileCompressInfo(segDir, "index/index1/dictionary", expectCompressType);
        CheckFileCompressInfo(segDir, "index/index2_section/offset", expectCompressType);
        CheckFileCompressInfo(segDir, "index/index2_section/data", expectCompressType);
        CheckFileCompressInfo(segDir, "attribute/price/data", expectCompressType);
        CheckFileCompressInfo(segDir, "attribute/string1/offset", expectCompressType);
        CheckFileCompressInfo(segDir, "attribute/string1/data", expectCompressType);
        CheckFileCompressInfo(segDir, "summary/data", expectCompressType);
        CheckFileCompressInfo(segDir, "summary/offset", expectCompressType);
        CheckFileCompressInfo(segDir, "source/group_0/data", expectCompressType);
        CheckFileCompressInfo(segDir, "source/group_0/offset", expectCompressType);
        CheckFileCompressInfo(segDir, "source/group_1/data", expectCompressType);
        CheckFileCompressInfo(segDir, "source/group_1/offset", expectCompressType);
    };

    // check build version
    auto partData = OnDiskPartitionData::CreateOnDiskPartitionData(psm.GetFileSystem());
    ASSERT_EQ(4, partData->GetVersion().GetSegmentCount());
    CheckSegmentCompressInfo(partData, 0, "null"); // segment 0 is hot (2 hot)
    CheckSegmentCompressInfo(partData, 1, "null"); // segment 1 is hot (1 hot : 1 warm)
    CheckSegmentCompressInfo(partData, 2, "lz4");  // segment 2 is warm (1 warm : 1 cold)
    CheckSegmentCompressInfo(partData, 3, "zstd"); // segment 4 is cold (2 cold)
    auto oldSegmentInfo = *(partData->GetSegmentData(0).GetSegmentInfo());
    uint64_t oldFingerPrint;
    ASSERT_TRUE(oldSegmentInfo.GetCompressFingerPrint(oldFingerPrint));

    // update file compress schema
    string patchStr = R"({
        "file_compress": [
            {
                "name":"new_cold_compressor",
                "type":"zlib"
            },
            {
                "name":"new_warm_compressor",
                "type":"snappy"
            },
            {
                "name":"temperature_compressor",
            "type":"",
            "temperature_compressor":
            {
            "warm" : "new_warm_compressor",
            "COLD"  : "new_cold_compressor"
            }
        }
        ],
        "table_name": "noname"
    })";
    UpdateableSchemaStandardsPtr patchStandards(new UpdateableSchemaStandards);
    FromJsonString(*patchStandards, patchStr);
    options.SetUpdateableSchemaStandards(*patchStandards);

    string incDocString = "cmd=add,pk=1,string1=hello,price=4,string2=doc;"    // hot
                          "cmd=add,pk=3,string1=hello,price=6,string2=cat;"    // hot
                          "cmd=add,pk=4,string1=hello,price=10;"               // warm
                          "cmd=add,pk=5,string1=hello,price=12,string2=abc;"   // warm
                          "cmd=add,pk=8,string1=hello,price=400,string2=dog;"; // cold
    PartitionStateMachine newPsm;
    INDEXLIB_TEST_TRUE(newPsm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(newPsm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));

    // check new build version
    partData = OnDiskPartitionData::CreateOnDiskPartitionData(newPsm.GetFileSystem());
    CheckSegmentCompressInfo(partData, 4, "null");   // segment 4 is hot (2 hot)
    CheckSegmentCompressInfo(partData, 5, "snappy"); // segment 5 is hot (1 hot : 1 warm)
    CheckSegmentCompressInfo(partData, 6, "zlib");   // segment 6 is warm (1 warm : 1 cold)

    INDEXLIB_TEST_TRUE(newPsm.Transfer(BUILD_INC, "", "", ""));
    // check latest version : optimized merge split by temperature
    partData = OnDiskPartitionData::CreateOnDiskPartitionData(newPsm.GetFileSystem());
    auto version = partData->GetVersion();
    ASSERT_EQ(3, version.GetSegmentCount());
    CheckSegmentCompressInfo(partData, version[0], "null");   // segment 7 is hot
    CheckSegmentCompressInfo(partData, version[1], "zlib");   // segment 8 is warm
    CheckSegmentCompressInfo(partData, version[2], "snappy"); // segment 9 is cold
    auto newSegmentInfo = *(partData->GetSegmentData(version[0]).GetSegmentInfo());
    uint64_t newFingerPrint;
    ASSERT_TRUE(newSegmentInfo.GetCompressFingerPrint(newFingerPrint));
    ASSERT_NE(oldFingerPrint, newFingerPrint);
}

void OfflinePartitionInteTest::TestBuildSource()
{
    string field = "pk:string;string1:string;string2:string;price:uint32";
    string index = "pk:primarykey64:pk;index1:string:string1;index2:string:string2";
    string attribute = "string1;price";
    string source = "string1:pk#string2:price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "", "", source);
    SourceGroupConfigPtr groupConfig = schema->GetSourceSchema()->GetGroupConfig(1);
    string paramStr = R"({
        "compress_type" : "uniq|equal",
        "doc_compressor" : "zstd",
        "file_compressor" : "snappy",
        "enable_compress_offset" : true
    })";

    GroupDataParameter param;
    FromJsonString(param, paramStr);
    groupConfig->SetParameter(param);

    string docString = "cmd=add,pk=1,string1=hello,price=4;"
                       "cmd=add,pk=2,string1=hello,price=5;"
                       "cmd=add,pk=3,string1=hello,price=6,string2=cat;";

    mOptions.GetBuildConfig().maxDocCount = 2;
    mOptions.GetMergeConfig().mergeThreadCount = 8;
    PartitionStateMachine psm;

    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "index1:hello", "docid=0;docid=1;docid=2"));
    Version version;
    string versionFile = FslibWrapper::JoinPath(mRootDir, "version.0");
    version.Load(versionFile);
    ASSERT_EQ((size_t)2, version.GetSegmentCount());

    autil::mem_pool::Pool* pool = new autil::mem_pool::Pool(1024);

    {
        index::SourceReaderPtr sourceReader = psm.GetIndexPartition()->GetReader()->GetSourceReader();
        document::SourceDocument sourceDocument(pool);
        index::PrimaryKeyIndexReaderPtr pkReader = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
        docid_t docId = pkReader->Lookup("1");
        ASSERT_TRUE(sourceReader->GetDocument(docId, &sourceDocument));
        ASSERT_EQ(StringView("hello"), sourceDocument.GetField((sourcegroupid_t)0, "string1"));
        ASSERT_EQ(StringView("4"), sourceDocument.GetField((sourcegroupid_t)1, "price"));
        ASSERT_EQ(StringView::empty_instance(), sourceDocument.GetField((sourcegroupid_t)1, "string2"));

        ASSERT_TRUE(psm.Transfer(QUERY, "", "source_group:0:pk:1", "pk=1,string1=hello"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "source_group:1:pk:1", "price=4"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "source_group:0:pk:2", "pk=2,string1=hello"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "source_group:1:pk:2", "price=5"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "source_group:0:pk:3", "pk=3,string1=hello"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "source_group:1:pk:3", "price=6,string2=cat"));
    }
    {
        // test read concureently
        cout << "test read concureently" << endl;
        index::SourceReaderPtr sourceReader = psm.GetIndexPartition()->GetReader()->GetSourceReader();
        index::PrimaryKeyIndexReaderPtr pkReader = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
        docid_t docId = pkReader->Lookup("1");

        const int TEST_THREAD_COUNT = 4;
        vector<autil::ThreadPtr> readThreads(TEST_THREAD_COUNT);
        for (size_t i = 0; i < readThreads.size(); ++i) {
            readThreads[i] = autil::Thread::createThread([&pool, docId, &sourceReader]() {
                size_t times = 200;
                while (times--) {
                    document::SourceDocument sourceDocument(pool);
                    ASSERT_TRUE(sourceReader->GetDocument(docId, &sourceDocument));
                }
            });
        }
        for (auto& thread : readThreads) {
            thread->join();
        }
    }
    // test rt build
    {
        string rtDoc = "cmd=add,pk=4,string1=beijing,string2=haidian,price=100";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "source_group:0:pk:4", "pk=4,string1=beijing"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "source_group:1:pk:4", "price=100,string2=haidian"));
    }
    // test inc build
    {
        string incDoc = "cmd=add,pk=12,string1=guangzhou,string2=tianhe,price=999;" // add new doc
                        "cmd=add,pk=2,string1=tianjing,string2=wuqin,price=007";    // override doc[pk=2]
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
        index::SourceReaderPtr sourceReader = psm.GetIndexPartition()->GetReader()->GetSourceReader();
        index::PrimaryKeyIndexReaderPtr pkReader = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();

        docid_t docId = pkReader->Lookup("2");
        document::RawDocumentPtr rawDoc(new DefaultRawDocument);
        SourceDocumentPtr doc1(new SourceDocument(rawDoc->getPool()));
        ASSERT_TRUE(sourceReader->GetDocument(docId, doc1.get()));
        doc1->ToRawDocument(*rawDoc);
        ASSERT_EQ("2", rawDoc->getField("pk"));
        ASSERT_EQ("tianjing", rawDoc->getField("string1"));
        ASSERT_EQ("wuqin", rawDoc->getField("string2"));
        ASSERT_EQ("007", rawDoc->getField("price"));

        docId = pkReader->Lookup("12");
        document::RawDocumentPtr rawDoc2(new DefaultRawDocument);
        SourceDocumentPtr doc2(new SourceDocument(rawDoc2->getPool()));
        ASSERT_TRUE(sourceReader->GetDocument(docId, doc2.get()));
        doc2->ToRawDocument(*rawDoc2);
        ASSERT_EQ("12", rawDoc2->getField("pk"));
        ASSERT_EQ("guangzhou", rawDoc2->getField("string1"));
        ASSERT_EQ("tianhe", rawDoc2->getField("string2"));
        ASSERT_EQ("999", rawDoc2->getField("price"));

        // use psm check
        ASSERT_TRUE(psm.Transfer(QUERY, "", "source_group:0:pk:2", "string1=tianjing"));
        ASSERT_FALSE(psm.Transfer(QUERY, "", "source_group:0:pk:2", "price=007"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "source_group:1:pk:2", "string2=wuqin,price=007"));
        ASSERT_FALSE(psm.Transfer(QUERY, "", "source_group:1:pk:2", "string1=tianjing"));
        ASSERT_FALSE(psm.Transfer(QUERY, "", "source_group:1:pk:2", "string2=wuqin,price=7"));
    }
    delete pool;
}
}} // namespace indexlib::partition
