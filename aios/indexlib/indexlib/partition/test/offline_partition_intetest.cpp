#include "indexlib/partition/test/offline_partition_intetest.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OfflinePartitionInteTest);

OfflinePartitionInteTest::OfflinePartitionInteTest()
{
}

OfflinePartitionInteTest::~OfflinePartitionInteTest()
{
}

void OfflinePartitionInteTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    mOptions = options;
    mOptions.SetIsOnline(false);
    mOptions.GetBuildConfig().speedUpPrimaryKeyReader = std::tr1::get<0>(GET_CASE_PARAM());
    mOptions.GetBuildConfig().enablePackageFile = std::tr1::get<1>(GET_CASE_PARAM());
}

void OfflinePartitionInteTest::CaseTearDown()
{
}

void OfflinePartitionInteTest::TestAddDocument()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index1:string:string1;index2:string:string2";
    string attribute = "string1;price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index,
            attribute, "");

    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5;"
                       "cmd=add,string1=hello,price=6;";

    mOptions.GetBuildConfig().maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "index1:hello", 
                                    "docid=0;docid=1;docid=2"));
    Version version;
    string versionFile = FileSystemWrapper::JoinPath(mRootDir, "version.0");
    version.Load(versionFile);
    ASSERT_EQ((size_t)3, version.GetSegmentCount());
}

void OfflinePartitionInteTest::TestMergeErrorWithBuildFileLost()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index1:string:string1;index2:string:string2";
    string attribute = "string1;price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index,
            attribute, "");

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
        deleteStr = deleteStr.substr(0, deleteStr.length()-1);
        FileSystemWrapper::Rename(deleteStr, deleteStr + "__temp__");
        ASSERT_ANY_THROW(DoStepMerge("BEGIN_MERGE", info, true));
        FileSystemWrapper::Rename(deleteStr + "__temp__", deleteStr);
    }
    {
        string deleteStr = FileSystemWrapper::JoinPath(mRootDir, "segment_1_level_0");
        FileSystemWrapper::Rename(deleteStr, deleteStr + "__temp__");

        ASSERT_ANY_THROW(DoStepMerge("BEGIN_MERGE", info, true));
        FileSystemWrapper::Rename(deleteStr + "__temp__", deleteStr);
    }
    {
        string fileName = "segment_1_level_0/index";
        if (mOptions.GetBuildConfig().enablePackageFile)
        {
            fileName = "segment_1_level_0/package_file.__data__0";
        }
        string deleteStr = FileSystemWrapper::JoinPath(mRootDir, fileName);
        FileSystemWrapper::Rename(deleteStr, deleteStr + "__temp__");
        
        //ASSERT_ANY_THROW(DoStepMerge("BEGIN_MERGE", info, true));
        //todo delete index file throw exception
        FileSystemWrapper::Rename(deleteStr + "__temp__", deleteStr);
    }
    ASSERT_NO_THROW(DoStepMerge("BEGIN_MERGE", info, true));
    /*
    {
        string fileName = "segment_1_level_0/index";
        if (mOptions.GetBuildConfig().enablePackageFile)
        {
            fileName = "segment_1_level_0/package_file.__data__0";
        }
        string deleteStr = FileSystemWrapper::JoinPath(mRootDir, fileName);
        FileSystemWrapper::Rename(deleteStr, deleteStr + "__temp__");
        //ASSERT_ANY_THROW(DoStepMerge("DO_MERGE", info, true));
        //todo delete index file throw exception
        FileSystemWrapper::Rename(deleteStr + "__temp__", deleteStr);
    }
    ASSERT_NO_THROW(DoStepMerge("DO_MERGE", info, true));
    */
}

void OfflinePartitionInteTest::TestMergeErrorWithMergeFileLost()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index1:string:string1;index2:string:string2";
    string attribute = "string1;price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index,
            attribute, "");

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
        string deleteStr = FileSystemWrapper::JoinPath(mRootDir, fileName);
        FileSystemWrapper::Rename(deleteStr, deleteStr + "__temp__");
        ASSERT_ANY_THROW(DoStepMerge("DO_MERGE", info, true));
        FileSystemWrapper::Rename(deleteStr + "__temp__", deleteStr);
    }
    {
        string fileName = "segment_1_level_0/index";
        if (mOptions.GetBuildConfig().enablePackageFile)
        {
            fileName = "segment_1_level_0/package_file.__data__0";
        }
        string deleteStr = FileSystemWrapper::JoinPath(mRootDir, fileName);
        FileSystemWrapper::Rename(deleteStr, deleteStr + "__temp__");
        //ASSERT_ANY_THROW(DoStepMerge("DO_MERGE", info, true));
        //todo delete index file throw exception
        FileSystemWrapper::Rename(deleteStr + "__temp__", deleteStr);
    }
    ASSERT_NO_THROW(DoStepMerge("DO_MERGE", info, true));
    {
        string fileName = "instance_0/segment_3_level_0";
        string deleteStr = FileSystemWrapper::JoinPath(mRootDir, fileName);
        FileSystemWrapper::Rename(deleteStr, deleteStr + "__temp__");
        //ASSERT_ANY_THROW(DoStepMerge("END_MERGE", info, true));
        //todo delete segment file throw exception
        FileSystemWrapper::Rename(deleteStr + "__temp__", deleteStr);
    }
    {
        string fileName = "instance_0/segment_3_level_0/index";
        string deleteStr = FileSystemWrapper::JoinPath(mRootDir, fileName);
        FileSystemWrapper::Rename(deleteStr, deleteStr + "__temp__");
        //ASSERT_ANY_THROW(DoStepMerge("END_MERGE", info, true));
        //todo delete index file throw exception
        FileSystemWrapper::Rename(deleteStr + "__temp__", deleteStr);
    }
    ASSERT_NO_THROW(DoStepMerge("END_MERGE", info, true));
}

void OfflinePartitionInteTest::TestDelDocument()
{
    string docString = 
        "cmd=add,string1=hello1,string2=hello,price=4;"
        "cmd=add,string1=hello2,string2=hello,price=5;"
        "cmd=add,string1=hello3,string2=hello,price=6;"
        "cmd=delete,string1=hello3;";

    mOptions.GetBuildConfig().maxDocCount = 2;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "index2:hello", 
                                    "docid=0;docid=1"));
    
    string delDocString = "cmd=delete,string1=hello2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, delDocString, 
                                    "index2:hello", 
                                    "docid=0"));
}

void OfflinePartitionInteTest::TestDedupAddDocument()
{
    string docString = 
        "cmd=add,string1=hello1,string2=hello,price=4;"
        "cmd=add,string1=hello2,string2=hello,price=5;"
        "cmd=add,string1=hello3,string2=hello,price=6;"
        "cmd=add,string1=hello1,string2=hello,price=7;";

    mOptions.GetBuildConfig().maxDocCount = 2;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "index2:hello", 
                                    "docid=1;docid=2;docid=3"));

    string incDocString = 
        "cmd=add,string1=hello3,string2=hello,price=8;"
        "cmd=add,string1=hello3,string2=hello,price=9;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, 
                                    "index2:hello", 
                                    "docid=1;docid=3;docid=5"));
}

void OfflinePartitionInteTest::TestUpdateDocument()
{
    string docString = 
        "cmd=add,string1=hello1,string2=hello,price=4;"
        "cmd=update_field,string1=hello1,price=5;"
        "cmd=add,string1=hello2,string2=hello,price=6;"
        "cmd=add,string1=hello3,string2=hello,price=7;"
        "cmd=update_field,string1=hello2,price=8;";

    mOptions.GetBuildConfig().maxDocCount = 2;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "index2:hello", 
                                    "docid=0,price=5;docid=1,price=8;docid=2,price=7"));

    string incDocString = 
        "cmd=update_field,string1=hello3,price=9;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, 
                                    "index2:hello", 
                                    "docid=0,price=5;docid=1,price=8;docid=2,price=9"));
}

void OfflinePartitionInteTest::TestAddDocWithoutPkWhenSchemaHasPkIndex()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docString = "cmd=add,string2=hello,price=4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "index2:hello", ""));
}

void OfflinePartitionInteTest::TestBuildingSegmentDedup()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docString = "cmd=add,string1=hello1,string2=hello,price=4;"
                       "cmd=add,string1=hello1,string2=hello,price=5;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "index2:hello",
                                    "docid=1,price=5"));

    string incDocString = "cmd=add,string1=hello3,string2=hello,price=8;"
                          "cmd=add,string1=hello3,string2=hello,price=9;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, 
                                    "index2:hello",
                                    "docid=1,price=5;docid=3,price=9"));
}

void OfflinePartitionInteTest::TestBuiltSegmentsDedup()
{
    IndexConfigPtr pkConfig = mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    PrimaryKeyIndexConfigPtr config = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, pkConfig);
    config->SetPrimaryKeyIndexType(pk_sort_array);

    PartitionStateMachine psm;
    mOptions.GetBuildConfig().maxDocCount = 1;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docString = "cmd=add,string1=hello1,string2=hello,price=4;"
                       "cmd=add,string1=hello1,string2=hello,price=5;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "index2:hello", "docid=1,price=5"));

    string incDocString = "cmd=add,string1=hello1,string2=hello,price=8;"
                          "cmd=add,string1=hello1,string2=hello,price=9;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, 
                                    "index2:hello", "docid=3,price=9"));
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
    offlinePart.Open(GET_TEST_DATA_PATH(), "", mSchema, mOptions, 0);

    IndexPartitionReaderPtr reader = offlinePart.GetReader();
    Version version = reader->GetVersion();
    ASSERT_EQ((versionid_t)0, version.GetVersionId());
    offlinePart.Close();
}

void OfflinePartitionInteTest::TestRecoverFromLegacySegmentDirName()
{
    string indexDir = string(TEST_DATA_PATH) + "/compatible_index_for_segment_directory_name";
    string rootDir = GET_TEST_DATA_PATH() + "/part_dir";
    fslib::fs::FileSystem::copy(indexDir, rootDir);
    IndexPartitionOptions options;
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::LoadAndRewritePartitionSchema(rootDir, options);

    string versionPath = FileSystemWrapper::JoinPath(rootDir, "version.1");
    string segInfoPath = FileSystemWrapper::JoinPath(rootDir, "segment_2/segment_info");
    fslib::fs::FileSystem::remove(versionPath);
    fslib::fs::FileSystem::remove(segInfoPath);
    PartitionStateMachine psm;    
    ASSERT_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE,
                             "cmd=add,pk=3,string1=hello,price=3;",
                             "index:hello",
                             "price=0;price=1;price=3"));
}

void OfflinePartitionInteTest::TestRecoverFromLegacySegmentDirNameWithNoVersion()
{
    string indexDir = string(TEST_DATA_PATH) + "/compatible_index_for_segment_directory_name";
    string rootDir = GET_TEST_DATA_PATH() + "/part_dir";
    fslib::fs::FileSystem::copy(indexDir, rootDir);
    IndexPartitionOptions options;
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::LoadAndRewritePartitionSchema(rootDir, options);

    string versionPath0 = FileSystemWrapper::JoinPath(rootDir, "version.0");
    string versionPath1 = FileSystemWrapper::JoinPath(rootDir, "version.1");
    string segInfoPath = FileSystemWrapper::JoinPath(rootDir, "segment_2/segment_info");
    fslib::fs::FileSystem::remove(versionPath0);
    fslib::fs::FileSystem::remove(versionPath1);
    fslib::fs::FileSystem::remove(segInfoPath);
    PartitionStateMachine psm;    
    ASSERT_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE,
                             "cmd=add,pk=3,string1=hello,price=3;",
                             "index:hello",
                             "price=3"));
}

void OfflinePartitionInteTest::TestRecoverWithNoVersion()
{
    mOptions.GetBuildConfig().maxDocCount = 1;
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        string docString = 
            "cmd=add,string1=hello1,string2=hello,price=4;"
            "cmd=add,string1=hello2,string2=hello,price=5;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }
    string versionPath0 = FileSystemWrapper::JoinPath(mRootDir, "version.0");
    string segInfoPath = FileSystemWrapper::JoinPath(mRootDir, "segment_1_level_0/segment_info");
    fslib::fs::FileSystem::remove(versionPath0);
    fslib::fs::FileSystem::remove(segInfoPath);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = 
        "cmd=add,string1=hello3,string2=hello,price=6;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "index2:hello",
                             "price=4;price=6"));
}

void OfflinePartitionInteTest::HasMetricsValue(
        const PartitionStateMachine& psm, const string &name, 
        kmonitor::MetricType type, const string &unit)
{
    misc::MetricProviderPtr provider = psm.GetMetricsProvider();
    misc::MetricPtr metric = provider->DeclareMetric(name, type);
    ASSERT_TRUE(metric != nullptr);
    ASSERT_NE(std::numeric_limits<double>::min(), metric->mValue);
}

void OfflinePartitionInteTest::DoStepMerge(
    const string& step, const ParallelBuildInfo& info, bool optimize)
{
    unique_ptr<merger::IndexPartitionMerger> merger(
            (merger::IndexPartitionMerger*)merger::PartitionMergerCreator::CreateIncParallelPartitionMerger(
                    mRootDir, info, mOptions, NULL, ""));
    merger::MergeMetaPtr meta = merger->CreateMergeMeta(optimize, 1, 0);
    if (step == "BEGIN_MERGE")
    {
        merger->PrepareMerge(0);
        return;
    }
    if (step == "DO_MERGE")
    {
        merger->DoMerge(optimize, meta, 0);
        return;
    }
    if (step == "END_MERGE")
    {
        merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());
        return;
    }
    ASSERT_TRUE(false);
}

IE_NAMESPACE_END(partition);

