#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/partition/test/index_builder_intetest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index/normal/attribute/attribute_update_info.h"
#include "indexlib/index/normal/attribute/accessor/multi_pack_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/common/file_system_factory.h"
#include "indexlib/config/impl/merge_config_impl.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/index/lifecycle_segment_metrics_updater.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "autil/legacy/json.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexBuilderInteTest);

IndexBuilderInteTest::IndexBuilderInteTest()
{
}

IndexBuilderInteTest::~IndexBuilderInteTest()
{
}

void IndexBuilderInteTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mQuotaControl.reset(new util::QuotaControl(1024 * 1024 * 1024));
}

void IndexBuilderInteTest::CaseTearDown()
{
}

void IndexBuilderInteTest::TestUniqSectionBuildAndMerge()
{
    string field = "field1:text;string2:string;price:uint32";
    string index = "index2:pack:field1;"
                   "pk:primarykey64:string2";
    //TODO: schema support compressed section config
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    SectionAttributeConfigPtr sectionConfig(
            new SectionAttributeConfig("uniq", false, false));
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index2");
    PackageIndexConfigPtr packIndexConfig =
        DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfig);
    packIndexConfig->SetSectionAttributeConfig(sectionConfig);
    packIndexConfig->SetHasSectionAttributeFlag(true);

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    BuildConfig& buildConfig = options.GetBuildConfig();
    buildConfig.enablePackageFile = false;
    buildConfig.maxDocCount = 1;
    IndexBuilderPtr indexBuilder(new IndexBuilder(GET_TEST_DATA_PATH(), options,
                    schema, mQuotaControl));
    ASSERT_TRUE(indexBuilder->Init());

    string docString = "cmd=add,string2=0,field1=hello0,locator=0,ts=1;"
                       "cmd=add,string2=1,field1=hello0,locator=2,ts=2;";
    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docString);
    indexBuilder->Build(docs[0]);
    indexBuilder->Build(docs[1]);
    indexBuilder->Merge(options);

    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    rootDir->GetDirectory("segment_0_level_0", true)->MountPackageFile(PACKAGE_FILE_PREFIX);
    rootDir->GetDirectory("segment_1_level_0", true)->MountPackageFile(PACKAGE_FILE_PREFIX);
    rootDir->GetDirectory("segment_2_level_0", true)->MountPackageFile(PACKAGE_FILE_PREFIX);

    string sectionDataPath0 = "/segment_0_level_0/index/index2_section/data";
    string sectionDataPath1 = "/segment_1_level_0/index/index2_section/data";
    // merged segment
    string sectionDataPath2 = "/segment_2_level_0/index/index2_section/data";

    ASSERT_EQ(rootDir->GetFileLength(sectionDataPath0),
              rootDir->GetFileLength(sectionDataPath1));

    ASSERT_EQ(rootDir->GetFileLength(sectionDataPath0),
              rootDir->GetFileLength(sectionDataPath2));
}

void IndexBuilderInteTest::TestGetIncVersionTimestamp()
{
    //test offline empty
    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 1;
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    IndexBuilderPtr offlineIndexBuilder(new IndexBuilder(GET_TEST_DATA_PATH(),
                    options, mSchema, mQuotaControl));
    ASSERT_TRUE(offlineIndexBuilder->Init());

    offlineIndexBuilder->EndIndex();

    IndexPartitionPtr partition(new OnlinePartition);
    partition->Open(GET_TEST_DATA_PATH(), "", mSchema, options);
    IndexBuilder onlineIndexBuilder(partition, mQuotaControl);
    ASSERT_TRUE(onlineIndexBuilder.Init());
    ASSERT_EQ(-1, onlineIndexBuilder.GetIncVersionTimestamp());

    //test inc cover no rt
    string docString = "cmd=add,string1=hello0,locator=0,ts=1;"
                       "cmd=add,string1=hello1,locator=2,ts=2;";

    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(mSchema, docString);
    offlineIndexBuilder.reset(new IndexBuilder(GET_TEST_DATA_PATH(), options,
                    mSchema, mQuotaControl));
    ASSERT_TRUE(offlineIndexBuilder->Init());

    offlineIndexBuilder->Build(docs[0]);
    offlineIndexBuilder->Merge(options);

    partition->ReOpen(false);
    ASSERT_EQ(1, onlineIndexBuilder.GetIncVersionTimestamp());

    //test add realtime doc in mem
    onlineIndexBuilder.Build(docs[1]);
    ASSERT_EQ(1, onlineIndexBuilder.GetIncVersionTimestamp());
}

void IndexBuilderInteTest::TestGetLastLocatorForOffline()
{
    //test offline get last locator
    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 2;
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    IndexBuilder indexBuilder(GET_TEST_DATA_PATH(), options, mSchema, mQuotaControl);
    ASSERT_TRUE(indexBuilder.Init());

    ASSERT_TRUE(indexBuilder.GetLastLocator().empty());

    string docString = "cmd=add,string1=hello0,locator=0;"
                       "cmd=add,string1=hello1,locator=1;"
                       "cmd=add,string1=hello2,locator=2;"
                       "cmd=add,string1=hello3,locator=;"
                       "cmd=add,string1=hello2,locator=;";

    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(mSchema, docString);
    indexBuilder.Build(docs[0]);
    ASSERT_EQ(string("0"), indexBuilder.GetLastLocator());

    indexBuilder.Build(docs[1]);
    ASSERT_EQ(string("1"), indexBuilder.GetLastLocator());

    indexBuilder.Build(docs[2]);
    ASSERT_EQ(string("2"), indexBuilder.GetLastLocator());

    indexBuilder.Build(docs[3]);
    ASSERT_EQ(string("2"), indexBuilder.GetLastLocator());

    indexBuilder.Build(docs[4]);
    ASSERT_EQ(string("2"), indexBuilder.GetLastLocator());

    indexBuilder.EndIndex();
    GET_PARTITION_DIRECTORY()->RemoveFile("version.0");

    //test recover get last locator
    IndexBuilder recoverIndexBuilder(GET_TEST_DATA_PATH(), options, mSchema,
            mQuotaControl);
    ASSERT_TRUE(recoverIndexBuilder.Init());

    ASSERT_EQ(string("2"), recoverIndexBuilder.GetLastLocator());
}

void IndexBuilderInteTest::TestMerge()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5;"
                       "cmd=add,string1=hello,price=6;";
    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 1;
    auto raidConfig = options.GetOfflineConfig().buildConfig.GetRaidConfig();
    raidConfig->useRaid = true;
    raidConfig->bufferSizeThreshold = 0;
    options.GetOfflineConfig().mergeConfig.SetEnablePackageFile(false);
    std::string mergeConfigStr
        = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);


    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                                    "pk:hello",
                                    "docid=0,price=6"));
}

void IndexBuilderInteTest::TestAddToUpdate()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5,modify_fields=price;"
                       "cmd=add,string1=hello1,price=6;"
                       "cmd=add,string1=hello1,price=7,modify_fields=string2#price;";
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE,
                                    docString, "pk:hello",
                                    "docid=0,price=5"));

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE,
                                    "", "pk:hello1",
                                    "docid=2,price=7"));
}

void IndexBuilderInteTest::TestMergeCleanVersion()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 1;
    options.GetBuildConfig().keepVersionCount = 1;
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));

    string fullDocString = "cmd=add,string1=hello,price=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE,
                             fullDocString, "pk:hello",
                             "docid=0,price=4"));


    IndexBuilder indexBuilder(GET_TEST_DATA_PATH(), options, mSchema, mQuotaControl);
    ASSERT_TRUE(indexBuilder.Init());
    string incDocString = "cmd=add,string1=hello1,price=1";

    NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, incDocString);
    indexBuilder.Build(doc);
    indexBuilder.Merge(options, true);
    DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(directory->IsExist("version.2"));
    ASSERT_TRUE(!directory->IsExist("version.0"));
    ASSERT_TRUE(!directory->IsExist("version.1"));
}

void IndexBuilderInteTest::TestCleanVersionsWithReservedVersionList()
{
    IndexPartitionOptions options;
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    {
        PartitionStateMachine psm;
        options.SetIsOnline(false);
        options.GetBuildConfig().maxDocCount = 1;
        options.GetBuildConfig().keepVersionCount = 1;
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));

        string fullDocString = "cmd=add,string1=hello,price=4;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE,
                                 fullDocString, "pk:hello",
                                 "docid=0,price=4"));
    }
    {
        IndexBuilder indexBuilder(GET_TEST_DATA_PATH(), options, mSchema, mQuotaControl);
        ASSERT_TRUE(indexBuilder.Init());
        string incDocString = "cmd=add,string1=hello1,price=1";

        NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, incDocString);
        indexBuilder.Build(doc);
        indexBuilder.Merge(options, true);
        DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        ASSERT_TRUE(directory->IsExist("version.2"));
        ASSERT_TRUE(!directory->IsExist("version.0"));
        ASSERT_TRUE(!directory->IsExist("version.1"));
    }
    {
        options.SetReservedVersions({2});
        IndexBuilder indexBuilder(GET_TEST_DATA_PATH(), options, mSchema, mQuotaControl);
        ASSERT_TRUE(indexBuilder.Init());
        string incDocString = "cmd=add,string1=hello2,price=2";

        NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, incDocString);
        indexBuilder.Build(doc);
        indexBuilder.EndIndex();
        DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        ASSERT_TRUE(directory->IsExist("version.2"));
        ASSERT_TRUE(directory->IsExist("version.3"));
    }
    {
        options.SetReservedVersions({2,3});
        IndexBuilder indexBuilder(GET_TEST_DATA_PATH(), options, mSchema, mQuotaControl);
        ASSERT_TRUE(indexBuilder.Init());
        string incDocString = "cmd=add,string1=hello3,price=3";

        NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, incDocString);
        indexBuilder.Build(doc);
        indexBuilder.Merge(options, true);
        DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        ASSERT_TRUE(directory->IsExist("version.2"));
        ASSERT_TRUE(directory->IsExist("version.3"));
        ASSERT_TRUE(directory->IsExist("version.5"));
    }
}

void IndexBuilderInteTest::TestBuildAllSummaryInAttribute()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price";
    string summary = "price";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, summary);

    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);


    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    string fullDocString = "cmd=add,string1=hello,price=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL,
                                    fullDocString, "pk:hello",
                                    "docid=0,price=4"));
}

void IndexBuilderInteTest::TestUpdateDeletedDoc()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    //segment_0: doc 0, 1
    //segment_1: delete 0, update 0
    //segment_2: add 3, delete 3, update 3, update 1
    string docString = "0,1#"
                       "delete:0,update_field:0:2#"
                       "3,delete:2,update_field:2:4,update_field:1:5";
    provider.Build(docString, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    SingleValueAttributeReader<int32_t> attrReader;
    attrReader.Open(provider.GetAttributeConfig(), partitionData);
    PatchLoader::LoadAttributePatch(attrReader, provider.GetAttributeConfig(),
                                    partitionData);
    int32_t value = 0;
    attrReader.Read(0, value);
    ASSERT_EQ((int32_t)0, value);
    attrReader.Read(1, value);
    ASSERT_EQ((int32_t)5, value);
    attrReader.Read(2, value);
    ASSERT_EQ((int32_t)3, value);
}

void IndexBuilderInteTest::TestOptimizeMergeWithNoSegmentsToMerge()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    BuildConfig& buildConfig = options.GetBuildConfig();
    MergeConfig& mergeConfig = options.GetMergeConfig();

    buildConfig.maxDocCount = 2;
    buildConfig.keepVersionCount = 1;
    mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
    mergeConfig.mergeStrategyParameter.SetLegacyString("max-doc-count=1");

    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));

    string fullDocString = "cmd=add,string1=hello,price=4;"
                           "cmd=add,string1=hello1,price=41;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC,
                                    fullDocString, "pk:hello",
                                    "docid=0,price=4"));
}

void IndexBuilderInteTest::CheckFileContent(
        const string& filePath, const string& fileContent)
{
    sleep(2);
    ASSERT_TRUE(FileSystemWrapper::IsExist(filePath));
    string actualContent;
    FileSystemWrapper::AtomicLoad(filePath, actualContent);
    ASSERT_EQ(fileContent, actualContent);
}

void IndexBuilderInteTest::TestInit()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    IndexBuilderPtr indexBuilder(new IndexBuilder(GET_TEST_DATA_PATH(), options,
                    mSchema, mQuotaControl));
    ASSERT_TRUE(indexBuilder->Init());
    ASSERT_TRUE(indexBuilder->GetLock() != NULL);
}

void IndexBuilderInteTest::TestLoadIndexFormatVersion()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().enablePackageFile = false;
    IndexBuilderPtr indexBuilder(new IndexBuilder(TEST_DATA_PATH, options,
                    mSchema, mQuotaControl));
    ASSERT_TRUE(indexBuilder->Init());
    ASSERT_TRUE(indexBuilder->GetIsLegacyIndexVersion());
}

void IndexBuilderInteTest::TestGetLastLocator()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().enablePackageFile = false;
    IndexBuilderPtr indexBuilder(new IndexBuilder(TEST_DATA_PATH, options,
                    mSchema, mQuotaControl));
    indexBuilder->SetIsLegacyIndexVersion(false);
    ASSERT_TRUE(indexBuilder->Init());
    INDEXLIB_TEST_EQUAL("", indexBuilder->GetLastLocator());
}

void IndexBuilderInteTest::TestEndVersionWithEmptyData()
{
    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 1;

    IndexBuilderPtr offlineIndexBuilder(
            new IndexBuilder(GET_TEST_DATA_PATH(), options, mSchema,
                    mQuotaControl));
    ASSERT_TRUE(offlineIndexBuilder->Init());
    offlineIndexBuilder->EndIndex();

    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(rootDirectory->IsExist(SCHEMA_FILE_NAME));
    ASSERT_TRUE(rootDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME));
    ASSERT_TRUE(rootDirectory->IsExist("version.0"));

    Version version;
    VersionLoader::GetVersion(rootDirectory, version, INVALID_VERSION);

    ASSERT_EQ((versionid_t)0, version.GetVersionId());
    ASSERT_EQ((uint32_t)0, version.GetSegmentCount());
    ASSERT_EQ((int64_t)INVALID_TIMESTAMP, version.GetTimestamp());
}


void IndexBuilderInteTest::TestMergeWithAdaptiveBitmap()
{
    InnerTestMergeWithAdaptiveBitmap(true);
    InnerTestMergeWithAdaptiveBitmap(true, true);
    InnerTestMergeWithAdaptiveBitmap(false);
}

void IndexBuilderInteTest::TestBuildAndMergeEnablePackageFile()
{
    InnerTestBuildAndMergeEnablePackageFile(true);
    InnerTestBuildAndMergeEnablePackageFile(false);
}

void IndexBuilderInteTest::InnerTestBuildAndMergeEnablePackageFile(
        bool optimizeMerge)
{
    TearDown();
    SetUp();

    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "string1;price";
    string summary = "string2";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, summary);
    string docString = "cmd=add,string1=pk1,string2=hello,price=4;"
                       "cmd=add,string1=pk2,string2=hello,price=5;"
                       "cmd=add,string1=pk3,string2=hello,price=6;"
                       "cmd=update_field,string1=pk3,price=7;"
                       "cmd=delete,string1=pk2;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 1;
    options.GetBuildConfig().enablePackageFile = true;
    options.GetBuildConfig().dumpThreadCount = 2;

    if (!optimizeMerge)
    {
        options.GetMergeConfig().mergeStrategyStr = "specific_segments";
        options.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
                "merge_segments=1,3");
    }

    string rootDir = GET_TEST_DATA_PATH();
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                                    "index2:hello",
                                    "docid=0,string2=hello,string1=pk1,price=4;"
                                    "docid=2,string2=hello,string1=pk3,price=7"));

    CheckPackgeFile(rootDir + "segment_0_level_0");
    CheckPackgeFile(rootDir + "segment_1_level_0");
    CheckPackgeFile(rootDir + "segment_2_level_0");
    CheckPackgeFile(rootDir + "segment_3_level_0");

    // do merge
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "index2:hello",
                                    "docid=0,string2=hello,string1=pk1,price=4;"
                                    "docid=1,string2=hello,string1=pk3,price=7"));
    if (!optimizeMerge)
    {
        DirectoryPtr segmentDirectory = DirectoryCreator::Create(rootDir + "segment_4_level_0");
        segmentDirectory->MountPackageFile(PACKAGE_FILE_PREFIX);
        ASSERT_TRUE(segmentDirectory->IsExist("attribute/price/3_2.patch"));
    }
}

void IndexBuilderInteTest::TestOnlineDumpOperation()
{
    IndexPartitionOptions options;
    options.GetBuildConfig().enablePackageFile = false;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;

    string docString = "cmd=add,string1=hello,locator=-1,ts=4;"
                       "cmd=add,string1=hello0,locator=0,ts=3;"
                       "cmd=add,string1=hello,price=5,ts=6;";

    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(mSchema, docString);

    IndexBuilderPtr offlineIndexBuilder(
        new IndexBuilder(GET_TEST_DATA_PATH(), options, mSchema, mQuotaControl));
    ASSERT_TRUE(offlineIndexBuilder->Init());

    offlineIndexBuilder->Build(docs[0]);
    offlineIndexBuilder->Build(docs[1]);
    offlineIndexBuilder->EndIndex();

    IndexPartitionPtr partition(new OnlinePartition);
    partition->Open(GET_TEST_DATA_PATH(), "", mSchema, options);

    IndexBuilderPtr onlineBuilder(new IndexBuilder(partition, mQuotaControl));
    ASSERT_TRUE(onlineBuilder->Init());
    onlineBuilder->Build(docs[2]);
    onlineBuilder->DumpSegment();
    onlineBuilder.reset();

    partition->GetRootDirectory()->GetFileSystem()->Sync(true);

    DirectoryPtr rtDirectory = GET_PARTITION_DIRECTORY()->GetDirectory(
            RT_INDEX_PARTITION_DIR_NAME, true);

    // rt will dump operation queue
    ASSERT_TRUE(rtDirectory);

    segmentid_t rtSegId =
        RealtimeSegmentDirectory::ConvertToRtSegmentId(0);
    Version version;
    DirectoryPtr rtSegDirectory = rtDirectory->GetDirectory(
            version.GetNewSegmentDirName(rtSegId), true);
    ASSERT_TRUE(rtSegDirectory);
    ASSERT_TRUE(rtSegDirectory->IsExist("/operation_log/data"));
}

void IndexBuilderInteTest::TestBuildPackAttribute()
{
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::LoadSchema(string(TEST_DATA_PATH)+"pack_attribute/",
                                  "main_schema_with_pack.json");
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);

    IndexBuilderPtr offlineBuilder(
        new IndexBuilder(GET_TEST_DATA_PATH(), options, schema, mQuotaControl));
    ASSERT_TRUE(offlineBuilder->Init());

    string docStr = "cmd=add,int8_single=0,int8_multi=0 1,str_single=pk";
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(schema, docStr);
    ASSERT_TRUE(offlineBuilder->Build(doc));
    offlineBuilder->EndIndex();

    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr segDir = partDirectory->GetDirectory("segment_0_level_0", true);
    ASSERT_TRUE(segDir);
    DirectoryPtr attrDir = segDir->GetDirectory(ATTRIBUTE_DIR_NAME, true);
    ASSERT_TRUE(attrDir);
    DirectoryPtr packAttrDir = attrDir->GetDirectory("pack_attr", true);
    ASSERT_TRUE(packAttrDir);
    ASSERT_TRUE(packAttrDir->IsExist(ATTRIBUTE_OFFSET_FILE_NAME));
    ASSERT_TRUE(packAttrDir->IsExist(ATTRIBUTE_DATA_FILE_NAME));
}

void IndexBuilderInteTest::TestMergeAddDocWithPackAttribute()
{
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::LoadSchema(string(TEST_DATA_PATH)+"pack_attribute/",
                                  "main_schema_with_pack.json");

    IndexPartitionSchemaPtr subSchema =
        SchemaAdapter::LoadSchema(string(TEST_DATA_PATH)+"pack_attribute/",
                                  "sub_schema_with_pack.json");

    schema->SetSubIndexPartitionSchema(subSchema);

    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    options.GetBuildConfig(false).maxDocCount = 1;
    // set speedupPrimaryKeyReader = true, so there will be only 3 segments
    options.GetBuildConfig(false).speedUpPrimaryKeyReader = true;

    string docString = "cmd=add,int8_single=0,int8_multi=0 1,str_single=pk,"
        "sub_int8_single=36^37,sub_int8_multi=4 5^6 7,sub_str_single=subpk1^subpk2;"
        "cmd=add,int8_single=0,int8_multi=0 1,str_single=pk,"
        "sub_int8_single=38^39,sub_int8_multi=8 9^10 11,sub_str_single=subpk3^subpk4;"
        "cmd=add,int8_single=0,int8_multi=0 1,str_single=pk1,"
        "sub_int8_single=3^6,sub_int8_multi=4 5^2 4,sub_str_single=subpk5^subpk6;";

    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docString);

    IndexBuilder indexBuilder(GET_TEST_DATA_PATH(), options, schema, mQuotaControl);
    ASSERT_TRUE(indexBuilder.Init());

    for (size_t i = 0; i < docs.size(); ++i)
    {
        indexBuilder.Build(docs[i]);
    }
    indexBuilder.Merge(options, true);

    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr segDir = partDirectory->GetDirectory("segment_3_level_0", true);
    ASSERT_TRUE(segDir);
    segDir->MountPackageFile(PACKAGE_FILE_PREFIX);
    DirectoryPtr attrDir = segDir->GetDirectory(ATTRIBUTE_DIR_NAME, true);
    ASSERT_TRUE(attrDir);
    DirectoryPtr packAttrDir = attrDir->GetDirectory("pack_attr", true);
    ASSERT_TRUE(packAttrDir);
    ASSERT_TRUE(packAttrDir->IsExist(ATTRIBUTE_OFFSET_FILE_NAME));
    ASSERT_TRUE(packAttrDir->IsExist(ATTRIBUTE_DATA_FILE_NAME));

    DirectoryPtr subSegDir = segDir->GetDirectory("sub_segment", true);
    ASSERT_TRUE(subSegDir);
    subSegDir->MountPackageFile(PACKAGE_FILE_PREFIX);
    attrDir = subSegDir->GetDirectory(ATTRIBUTE_DIR_NAME, true);
    ASSERT_TRUE(attrDir);
    packAttrDir = attrDir->GetDirectory("sub_pack_attr", true);
    ASSERT_TRUE(packAttrDir);
    ASSERT_TRUE(packAttrDir->IsExist(ATTRIBUTE_OFFSET_FILE_NAME));
    ASSERT_TRUE(packAttrDir->IsExist(ATTRIBUTE_DATA_FILE_NAME));
}

void IndexBuilderInteTest::TestMergeUpdateDocWithPackAttribute()
{
    InnerTestMergeUpdateDocWithPackAttribute(false, false);
    InnerTestMergeUpdateDocWithPackAttribute(true, false);
    InnerTestMergeUpdateDocWithPackAttribute(false, true);
    InnerTestMergeUpdateDocWithPackAttribute(true, true);
}

void IndexBuilderInteTest::InnerTestMergeUpdateDocWithPackAttribute(
    bool sortBuild, bool uniq)
{
    TearDown();
    SetUp();

    string field = "string1:string;id:uint32;string2:string:false:true;price:uint32";
    string index = "pk:primarykey64:string1";
    string attribute = "id;packAttr:string2,price:";
    attribute += uniq ? "uniq" : "equal";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetMergeConfig().mergeStrategyStr = "specific_segments";
    options.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
            "merge_segments=1,2,3");
    options.GetBuildConfig().keepVersionCount = 10;
    options.GetBuildConfig().enablePackageFile = false;

    if (sortBuild)
    {
        PartitionMeta partitionMeta;
        partitionMeta.AddSortDescription("id", sp_asc);
        partitionMeta.Store(GET_PARTITION_DIRECTORY());
    }
    PartitionStateMachine psm;
    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    string fullDoc = "cmd=add,string1=pk0,id=0,string2=hello0,price=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));
    ASSERT_TRUE(partDir->IsExist("segment_0_level_0/attribute/packAttr/data_info"));

    string incDoc1 = "cmd=update_field,string1=pk0,string2=hello1;"
        "cmd=add,string1=pk1,id=4,string2=dog,price=66;"
        "cmd=add,string1=pk2,id=8,string2=cat,price=66;"
        "cmd=add,string1=pk3,id=10,string2=dog,price=66;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc1,
                             "pk:pk0", "string2=hello1,price=10"));

    ASSERT_TRUE(partDir->IsExist("segment_1_level_0/attribute/packAttr/string2/1_0.patch"));
    CheckUpdateInfo(partDir, "segment_1_level_0/attribute/packAttr/update_info", "0:1");

    string incDoc2 = "cmd=update_field,string1=pk1,string2=doggy;"
        "cmd=update_field,string1=pk3,string2=mice,price=11;"
        "cmd=add,string1=pk4,id=5,string2=dophin,price=77;"
        "cmd=add,string1=pk5,id=11,string2=mice,price=11;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc2, "pk:pk1", "string2=doggy,price=66"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk3", "string2=mice,price=11"));
    ASSERT_TRUE(partDir->IsExist("segment_2_level_0/attribute/packAttr/string2/2_1.patch"));
    ASSERT_TRUE(partDir->IsExist("segment_2_level_0/attribute/packAttr/price/2_1.patch"));

    CheckUpdateInfo(partDir, "segment_2_level_0/attribute/packAttr/update_info", "1:2");

    string incDoc3 = "cmd=update_field,string1=pk0,string2=hello3,price=22;"
        "cmd=update_field,string1=pk4,string2=dog,price=19;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc3, "pk:pk0", "string2=hello3,price=22"));
    if (partDir->IsExist("segment_4_level_0/package_file.__meta__"))
    {
        partDir->MountPackageFile("segment_4_level_0/package_file");
    }
    ASSERT_TRUE(partDir->IsExist("segment_4_level_0/attribute/packAttr/string2/3_0.patch"));
    ASSERT_TRUE(partDir->IsExist("segment_4_level_0/attribute/packAttr/price/3_0.patch"));
    ASSERT_TRUE(partDir->IsExist("segment_4_level_0/attribute/packAttr/data_info"));

    CheckUpdateInfo(partDir, "segment_4_level_0/attribute/packAttr/update_info", "0:1");

    if (sortBuild)
    {
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1", "docid=1,string2=doggy,price=66"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk2", "docid=3,string2=cat,price=66"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk3", "docid=4,string2=mice,price=11"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk4", "docid=2,string2=dog,price=19"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk5", "docid=5,string2=mice,price=11"));
    }
    else
    {
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1", "docid=1,string2=doggy,price=66"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk2", "docid=2,string2=cat,price=66"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk3", "docid=3,string2=mice,price=11"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk4", "docid=4,string2=dog,price=19"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk5", "docid=5,string2=mice,price=11"));
    }
}

void IndexBuilderInteTest::InnerTestOfflineBuildWithUpdatePackAttribute(
    bool uniqEncode, bool equalCompress)
{
    TearDown();
    SetUp();

    string field = "string1:string;string2:string:false:true;price:uint32;name:string:true:true";
    string index = "pk:primarykey64:string1";
    string attribute = "packAttr:string2,name,price";
    string compressStr = "";
    if (uniqEncode && equalCompress)
    {
        compressStr = ":uniq|equal";
    }
    else if (uniqEncode)
    {
        compressStr = ":uniq";
    }
    else if (equalCompress)
    {
        compressStr = ":equal";
    }

    if (!compressStr.empty())
    {
        attribute = attribute + compressStr;
    }

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().keepVersionCount = 10;
    options.GetBuildConfig().enablePackageFile = false;

    string fullDoc =
        "cmd=add,string1=pk0,string2=hello0,price=10,name=abc 123;"
        "cmd=update_field,string1=pk0,string2=hello1,price=11,name=cde 234;";  // update building segment

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE,
                             fullDoc, "pk:pk0",
                             "string2=hello1,price=11,name=cde 234"));

    // update built segment
    string incDoc =
        "cmd=update_field,string1=pk0,string2=hello2,price=12,name=efg 567;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc,
                             "pk:pk0", "string2=hello2,price=12,name=efg 567"));

    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(partDir->IsExist("segment_1_level_0/attribute/packAttr/string2/1_0.patch"));
    ASSERT_TRUE(partDir->IsExist("segment_1_level_0/attribute/packAttr/price/1_0.patch"));
    ASSERT_TRUE(partDir->IsExist("segment_1_level_0/attribute/packAttr/name/1_0.patch"));
    CheckUpdateInfo(partDir, "segment_1_level_0/attribute/packAttr/update_info", "0:1");
}

void IndexBuilderInteTest::InnerTestOnlineBuildWithUpdatePackAttribute(
    bool uniqEncode, bool equalCompress)
{
    TearDown();
    SetUp();

    string field = "string1:string;string2:string:false:true;price:uint32;name:string:true:true";
    string index = "pk:primarykey64:string1";
    string attribute = "packAttr:string2,name,price";
    string compressStr = "";
    if (uniqEncode && equalCompress)
    {
        compressStr = ":uniq|equal";
    }
    else if (uniqEncode)
    {
        compressStr = ":uniq";
    }
    else if (equalCompress)
    {
        compressStr = ":equal";
    }

    if (!compressStr.empty())
    {
        attribute = attribute + compressStr;
    }

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    options.SetIsOnline(true);
    options.GetBuildConfig().keepVersionCount = 10;
    options.GetBuildConfig().enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;

    string fullDoc =
        "cmd=add,string1=pk0,string2=hello0,price=10,name=abc pk0;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

    string rtDoc =
        "cmd=update_field,string1=pk0,string2=hello,price=1,name=cde pk0,ts=1;"
        "cmd=add,string1=pk2,string2=hello2,price=12,name=abc pk2,ts=1;"
        "cmd=update_field,string1=pk2,string2=hello3,name=ced pk2,ts=3;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDoc,
                             "pk:pk0", "string2=hello,price=1,name=cde pk0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "pk:pk2", "string2=hello3,price=12,name=ced pk2"));

    const IndexPartitionPtr& indexPartition = psm.GetIndexPartition();
    assert(indexPartition);
    const DirectoryPtr& rootDir = indexPartition->GetRootDirectory();
    DirectoryPtr partDir = rootDir->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, true);
    string updateInfoPath = string("segment_")
                            + StringUtil::toString(RealtimeSegmentDirectory::ConvertToRtSegmentId(0))
                            + "_level_0" +
                            + "/attribute/packAttr/update_info";

    CheckUpdateInfo(partDir, updateInfoPath, "0:1");

    string incDoc =
        "cmd=update_field,string1=pk0,string2=hello,price=1,name=efg pk0,ts=2;"
        "cmd=add,string1=pk2,string2=hello2,price=14,name=abc pk2,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc,
                             "pk:pk2", "string2=hello3,price=14,name=ced pk2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "pk:pk0", "string2=hello,price=1,name=efg pk0"));
    partDir = GET_PARTITION_DIRECTORY();
    CheckUpdateInfo(partDir, "segment_1_level_0/attribute/packAttr/update_info", "0:1");
}

void IndexBuilderInteTest::TestOfflineBuildWithUpdatePackAttribute()
{
    InnerTestOfflineBuildWithUpdatePackAttribute(true, true);
    InnerTestOfflineBuildWithUpdatePackAttribute(true, false);
    InnerTestOfflineBuildWithUpdatePackAttribute(false, true);
    InnerTestOfflineBuildWithUpdatePackAttribute(false, false);
}

void IndexBuilderInteTest::TestOnlineBuildWithUpdatePackAttribute()
{
    InnerTestOnlineBuildWithUpdatePackAttribute(true, true);
    InnerTestOnlineBuildWithUpdatePackAttribute(true, false);
    InnerTestOnlineBuildWithUpdatePackAttribute(false, true);
    InnerTestOnlineBuildWithUpdatePackAttribute(false, false);
}

void IndexBuilderInteTest::TestAddOverLengthPackAttribute()
{
    string field = "string1:string;string2:string:false:true;price:uint32";
    string index = "pk:primarykey64:string1;str_index:string:string2";
    string attribute = "pack_attr:string2,price";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");

    string docString = "cmd=add,string1=pk1,string2=hello,price=4;";

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", "");

    string overLenValue;
    overLenValue.resize(65535 * 1024, 'a');
    string overLenStr = "cmd=add,string1=pk2,string2=" + overLenValue + ",price=4;";
    string rtOverLenStr = "cmd=add,string1=pk2,string2=" + overLenValue + ",price=4,ts=10;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, overLenStr, "pk:pk2",
                             string("docid=1,string2=") + overLenValue));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, overLenStr, "pk:pk2",
                             string("docid=1,string2=") + overLenValue));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtOverLenStr, "pk:pk2",
                             string("docid=2,string2=") + overLenValue));
}

void IndexBuilderInteTest::TestUpdateOver64KPackAttribute()
{
    InnerTestUpdateOver64KPackAttribute(false);
    InnerTestUpdateOver64KPackAttribute(true);
}

void IndexBuilderInteTest::TestAutoAdd2Update()
{
    string field = "pk:string;price:uint32;info:string:false:true";
    string index = "pk:primarykey64:pk";
    string attribute = "price;info;pk";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    string docString = "cmd=add,pk=pk1,price=123,info=abc;"
                       "cmd=add,pk=pk1,price=234,info=abcd;";

    psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:pk1", "price=234,info=abcd");
    PartitionDataPtr partData = PartitionDataMaker::CreatePartitionData(
            GET_FILE_SYSTEM(), options, schema);

    SegmentData segData = partData->GetSegmentData(0);
    ASSERT_EQ((uint32_t)1, segData.GetSegmentInfo().docCount);
}

void IndexBuilderInteTest::TestCheckSortBuild()
{
    string fields = "attr1:uint32;attr2:uint64;pk:uint32";
    string indexs = "pk_index:primarykey64:pk";
    string attributes = "attr1;attr2";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(fields, indexs, attributes, "");

    PartitionMeta partMeta;
    partMeta.AddSortDescription("attr1", sp_desc);
    partMeta.AddSortDescription("attr2", sp_asc);
    partMeta.Store(GET_PARTITION_DIRECTORY());

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;

    string orderDocs = "cmd=add,pk=pk1,attr1=4,attr2=0;"
                       "cmd=add,pk=pk3,attr1=3,attr2=2;"
                       "cmd=add,pk=pk2,attr1=3,attr2=1;"
                       "cmd=add,pk=pk2,attr1=2,attr2=1;";

    string unorderDocs = "cmd=add,pk=pk1,attr1=4,attr2=0,ts=1;"
                         "cmd=add,pk=pk3,attr1=3,attr2=2,ts=2;"
                         "cmd=add,pk=pk2,attr1=2,attr2=1,ts=3;"
                         "cmd=add,pk=pk2,attr1=3,attr2=1,ts=4;";

    {
        // normal sequence
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

        ASSERT_TRUE(psm.Transfer(
                        BUILD_FULL_NO_MERGE, orderDocs, "", ""));

        ASSERT_TRUE(psm.Transfer(
                        BUILD_RT, unorderDocs, "", ""));
    }

    {
        // invalid sequence
        IndexPartitionOptions options;
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
        ASSERT_FALSE(psm.Transfer(
                        BUILD_FULL_NO_MERGE, unorderDocs, "", ""));
    }
}

void IndexBuilderInteTest::TestUnifiedVarNumAttribute()
{
    string fields = "uint32_attr:uint32:true;"
                    "uint64_attr:uint64:true:true;"
                    "str_field:string:false:true;"
                    "multi_str:string:true:true;"
                    "pk:uint32";
    string indexs = "pk:primarykey64:pk";
    string attributes = "uint32_attr;uint64_attr;str_field;multi_str";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(fields, indexs, attributes, "");

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDoc =
        "cmd=add,pk=1,uint32_attr=3 6,uint64_attr=1 2,str_field=abc,multi_str=efg 123;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "pk:1",
                             "uint32_attr=3 6,uint64_attr=1 2,str_field=abc,multi_str=efg 123"));

    // inc build
    string incDoc = "cmd=update_field,pk=1,uint64_attr=2 3,str_field=efghi,multi_str=efg 456 789;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "pk:1",
                             "uint32_attr=3 6,uint64_attr=2 3,str_field=efghi,multi_str=efg 456 789"));

    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "pk:1",
                             "uint32_attr=3 6,uint64_attr=2 3,str_field=efghi,multi_str=efg 456 789"));

    string rtDocs = "cmd=update_field,pk=1,uint64_attr=2 3 4,str_field=aefghi,multi_str=efg 789,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "pk:1",
                             "uint64_attr=2 3 4,str_field=aefghi,multi_str=efg 789"));
}

void IndexBuilderInteTest::TestUnifiedMultiString()
{
    string fields = "multi_str:string:true:true;"
                    "pk:uint32";
    string indexs = "pk:primarykey64:pk";
    string attributes = "multi_str";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(fields, indexs, attributes, "");

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDoc = "cmd=add,pk=1,multi_str=token38;"
                     "cmd=add,pk=2,multi_str=token38;"
                     "cmd=add,pk=3,multi_str=token38;"
                     "cmd=update_field,pk=2,multi_str=token39 token40";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "pk:2",
                             "multi_str=token39 token40"));

    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "pk:2", "multi_str=token39 token40"));
    string rtDoc = "cmd=update_field,pk=2,multi_str=token41,ts=10";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "pk:2", "multi_str=token41"));
}

void IndexBuilderInteTest::TestBuildMultiShardingIndex()
{
    string fields = "name:string;title:text;pk:string";
    string indexs = "pk_index:primarykey64:pk;"
                    "str_index:string:name:false:2;"
                    "pack_index:pack:title:false:2:true"; // use dict hash

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(fields, indexs, "", "");

    IndexPartitionOptions options;
    options.GetBuildConfig(true).enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDoc = "cmd=add,pk=0,name=yida,title=staff;"
                     "cmd=add,pk=1,name=pujian,title=abc;"
                     "cmd=add,pk=2,name=shoujing,title=def;"
                     "cmd=add,pk=3,name=yexiao,title=hig;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

    DirectoryPtr seg0Dir = GET_PARTITION_DIRECTORY()->GetDirectory(
            "segment_0_level_0", true);
    CheckIndexDir(seg0Dir, "str_index", 2, false);
    CheckIndexDir(seg0Dir, "pack_index", 2, true);

    string rtDoc = "cmd=add,pk=4,name=yida_1,title=staff_11,ts=1;"
                   "cmd=add,pk=5,name=pujian_2,title=abc_22,ts=2;"
                   "cmd=add,pk=6,name=shoujing_3,title=def_33,ts=3;"
                   "cmd=add,pk=7,name=yexiao_4,title=hig_44,ts=4;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDoc, "", ""));
    IndexPartitionPtr partition = psm.GetIndexPartition();
    DirectoryPtr rtDirectory = partition->GetRootDirectory()->GetDirectory(
            RT_INDEX_PARTITION_DIR_NAME, true);

    Version version;
    string rtSegmentDirName = version.GetNewSegmentDirName(
            RealtimeSegmentDirectory::ConvertToRtSegmentId(0));
    DirectoryPtr rtSegDir = rtDirectory->GetDirectory(rtSegmentDirName, false);
    ASSERT_TRUE(rtSegDir);
    CheckIndexDir(rtSegDir, "str_index", 2, false);
    CheckIndexDir(rtSegDir, "pack_index", 2, true);
}

void IndexBuilderInteTest::TestReadMultiShardingIndex()
{
    string fields = "name:string;title:text;pk:string";
    string indexs = "pk_index:primarykey64:pk;"
                    "str_index:string:name:false:2;"
                    "pack_index:pack:title:false:2:true"; // use dict hash

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(fields, indexs, "", "");

    IndexPartitionOptions options;
    options.GetBuildConfig(true).enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDoc = "cmd=add,pk=0,name=yida,title=staff;"
                     "cmd=add,pk=1,name=pujian,title=abc;"
                     "cmd=add,pk=2,name=shoujing,title=def;"
                     "cmd=add,pk=3,name=yexiao,title=hig;"
                     "cmd=add,pk=4,name=pujian,title=rst;";

    string rtDoc = "cmd=add,pk=5,name=yida_1,title=staff_11,ts=1;"
                   "cmd=add,pk=6,name=pujian,title=abc_22,ts=2;"
                   "cmd=add,pk=7,name=shoujing_3,title=def_33,ts=3;"
                   "cmd=add,pk=8,name=yexiao_4,title=hig,ts=4;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "str_index:pujian", "docid=1;docid=4"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "str_index:pujian", "docid=1;docid=4;docid=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pack_index:hig", "docid=3;docid=8"));
}

void IndexBuilderInteTest::TestMergeMultiShardingIndex()
{
    string fields = "name:string;title:text;pk:string";
    string indexs = "pk_index:primarykey64:pk;"
                    "str_index:string:name:false:2;"
                    "pack_index:pack:title:false:2:true"; // use dict hash

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(fields, indexs, "", "");

    IndexPartitionOptions options;
    options.GetBuildConfig(true).enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).keepVersionCount = 10;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    setenv("INDEXLIB_PRELOAD_DICTKEY_COUNT", "true", 1);

    // full build
    string fullDoc = "cmd=add,pk=0,name=yida,title=staff;"
                     "cmd=add,pk=1,name=pujian,title=abc;"
                     "cmd=add,pk=2,name=shoujing,title=def;"
                     "cmd=add,pk=3,name=yexiao,title=hig;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "", ""));

    DirectoryPtr seg0Dir = GET_PARTITION_DIRECTORY()->GetDirectory(
            "segment_0_level_0", true);
    CheckIndexDir(seg0Dir, "str_index", 2, false);
    CheckIndexDir(seg0Dir, "pack_index", 2, true);

    string incDoc = "cmd=add,pk=4,name=yida,title=staff_11,ts=1;"
                    "cmd=add,pk=5,name=pujian,title=abc,ts=2;"
                    "cmd=add,pk=6,name=shoujing_3,title=def_33,ts=3;"
                    "cmd=add,pk=7,name=yexiao_4,title=hig,ts=4;";

    setenv("INDEXLIB_PRELOAD_DICTKEY_COUNT", "false", 1);
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "str_index:yida", "docid=0;docid=4;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pack_index:hig", "docid=3;docid=7;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "str_index:pujian", "docid=1;docid=5;"));

    DirectoryPtr seg3Dir = GET_PARTITION_DIRECTORY()->GetDirectory(
            "segment_3_level_0", true);
    CheckIndexDir(seg3Dir, "str_index", 2, false);
    CheckIndexDir(seg3Dir, "pack_index", 2, true);
}

void IndexBuilderInteTest::CheckBitmapOnlyIndexData(
        const DirectoryPtr& indexDirectory,
        const IndexConfigPtr& indexConfig)
{
    const string& indexName = indexConfig->GetIndexName();
    DirectoryPtr indexDir = indexDirectory->GetDirectory(indexName, false);
    if (indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        ASSERT_FALSE(indexDir);
    }
    else
    {
        ASSERT_TRUE(indexDir);
        ASSERT_EQ((size_t)0, indexDir->GetFileLength(POSTING_FILE_NAME));
    }

    const vector<IndexConfigPtr>& shardingIndexConfigs =
        indexConfig->GetShardingIndexConfigs();
    for (size_t i = 0; i < shardingIndexConfigs.size(); i++)
    {
        CheckBitmapOnlyIndexData(indexDirectory, shardingIndexConfigs[i]);
    }
}

void IndexBuilderInteTest::CheckIndexDir(const DirectoryPtr& segDirectory,
        const string& indexName, size_t shardingCount, bool hasSection)
{
    assert(segDirectory);
    segDirectory->MountPackageFile(PACKAGE_FILE_PREFIX);
    DirectoryPtr indexDir = segDirectory->GetDirectory("index", false);
    ASSERT_TRUE(indexDir);

    DirectoryPtr normalIndexDir = indexDir->GetDirectory(indexName, false);
    if (shardingCount <= 1)
    {
        ASSERT_TRUE(normalIndexDir);
        ASSERT_TRUE(normalIndexDir->IsExist(DICTIONARY_FILE_NAME));
        ASSERT_TRUE(normalIndexDir->IsExist(POSTING_FILE_NAME));
    }
    else
    {
        ASSERT_FALSE(normalIndexDir) << indexName << "," << indexDir->GetPath();
    }

    for (size_t i = 0; i < shardingCount; ++i)
    {
        string shardingIndexName =
            IndexConfig::GetShardingIndexName(indexName, i);

        DirectoryPtr shardingIndexDir =
            indexDir->GetDirectory(shardingIndexName, false);
        ASSERT_TRUE(shardingIndexDir);

        ASSERT_TRUE(shardingIndexDir->IsExist(DICTIONARY_FILE_NAME));
        ASSERT_TRUE(shardingIndexDir->IsExist(POSTING_FILE_NAME));
        ASSERT_FALSE(indexDir->IsExist(
                        SectionAttributeConfig::IndexNameToSectionAttributeName(shardingIndexName)));
    }

    if (hasSection)
    {
        string sectionDirName = SectionAttributeConfig::IndexNameToSectionAttributeName(indexName);
        DirectoryPtr sectionDir = indexDir->GetDirectory(sectionDirName, false);
        ASSERT_TRUE(sectionDir);
        ASSERT_TRUE(sectionDir->IsExist(ATTRIBUTE_OFFSET_FILE_NAME));
        ASSERT_TRUE(sectionDir->IsExist(ATTRIBUTE_DATA_FILE_NAME));
    }
}

void IndexBuilderInteTest::InnerTestUpdateOver64KPackAttribute(bool uniqEncode)
{
    TearDown();
    SetUp();

    string field = "pk:string;string1:string:false:true;string2:string:false:true;price:uint32";
    string index = "pk:primarykey64:pk";
    string attribute = "pack_attr:string1,string2,price";
    if (uniqEncode)
    {
        attribute += ":uniq";
    }

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    string docString = "cmd=add,pk=pk1,string1=123,string2=hello,price=4;";
    psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", "");

    string overLenValue;
    overLenValue.resize(65535, 'a');

    string halfOverLenValue;
    halfOverLenValue.resize(32768, 'b');

    stringstream ss;
    ss << "cmd=update_field,pk=pk1,string1=" << halfOverLenValue << ";"
       << "cmd=update_field,pk=pk1,string2=" << halfOverLenValue << ";"
       << "cmd=add,pk=pk2,string1=" << halfOverLenValue << ",string2=345,price=6;"
       << "cmd=update_field,pk=pk2,string1=" << overLenValue << ";";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, ss.str(), "pk:pk2",
                             string("string1=") + overLenValue));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1",
                             string("string1=") + halfOverLenValue + ",string2=" + halfOverLenValue));

    // merge with patch
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "pk:pk2",
                             string("string1=") + overLenValue));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1",
                             string("string1=") + halfOverLenValue + ",string2=" + halfOverLenValue));

    stringstream rss;
    rss << "cmd=update_field,pk=pk1,string1=" << overLenValue << ",ts=1;"
        << "cmd=update_field,pk=pk1,string2=" << overLenValue << ",ts=2;"
        << "cmd=add,pk=pk3,string1=" << halfOverLenValue << ",string2=345,price=6,ts=3;"
        << "cmd=update_field,pk=pk3,string1=" << overLenValue << ",ts=4;";

    // RT build
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rss.str(), "pk:pk1",
                             string("string1=") + overLenValue + ",string2=" + overLenValue));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk3",
                             string("string1=") + overLenValue));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk3", "string2=345,price=6"));
}

// bugId : 8761515
void IndexBuilderInteTest::TestMaxTTLWithDelayDedup()
{
    string field = "string1:string;price:uint32";
    string index = "pk:primarykey64:string1";
    string attribute = "string1;price";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->SetEnableTTL(true);
    schema->SetAutoUpdatePreference(false);

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetMergeConfig().mergeStrategyStr = "specific_segments";
    options.GetMergeConfig().mergeStrategyParameter.SetLegacyString("merge_segments=4,5");

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // make doc not deleted by ttl
    int64_t ttl = TimeUtility::currentTimeInSeconds() + 1000;
    string ttl_field = DOC_TIME_TO_LIVE_IN_SECONDS +
                       "=" + StringUtil::toString<int64_t>(ttl);

    // seg0, seg1, seg2 -> seg3
    string fullDocs = "cmd=add,string1=pk1,price=1," + ttl_field + ";"
                      "cmd=add,string1=pk2,price=2," + ttl_field + ";"
                      "cmd=add,string1=pk3,price=3," + ttl_field + ";";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));

    // seg4, seg5, seg6(dedup_segment)
    string incDocs = "cmd=add,string1=pk1,price=4," + ttl_field + ";"
                     "cmd=add,string1=pk2,price=5," + ttl_field + ";";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));

    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist("segment_6_level_0/deletionmap/data_3"));

    // merge seg4, seg5 -> seg7
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));

    DeletionMapReaderPtr deletionMapReader =
        psm.GetIndexPartition()->GetReader()->GetDeletionMapReader();
    ASSERT_TRUE(deletionMapReader->IsDeleted(0));
    ASSERT_TRUE(deletionMapReader->IsDeleted(1));


    // update max ttl
    int64_t new_ttl = TimeUtility::currentTimeInSeconds() + 500;
    ttl_field = DOC_TIME_TO_LIVE_IN_SECONDS +
                "=" + StringUtil::toString<int64_t>(new_ttl);
    // seg8
    string updateIncDocs = "cmd=update_field,string1=pk3,price=6," + ttl_field + ";";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, updateIncDocs, "", ""));
    SegmentInfo segInfo;
    segInfo.Load(GET_PARTITION_DIRECTORY()->GetDirectory("segment_8_level_0", true));
    ASSERT_EQ(ttl, segInfo.maxTTL);

    // seg9
    string delIncDocs = "cmd=delete,string1=pk1," + ttl_field + ";";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, delIncDocs, "", ""));
    segInfo.Load(GET_PARTITION_DIRECTORY()->GetDirectory("segment_9_level_0", true));
    ASSERT_EQ(ttl, segInfo.maxTTL);
}

void IndexBuilderInteTest::TestShardingIndexBuildTruncateIndex()
{
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            string(TEST_DATA_PATH), "simple_sharding_example.json");
    string jsonStr;
    FileSystemWrapper::AtomicLoad(string(TEST_DATA_PATH) +
                                  "simple_truncate_for_sharding_index.json", jsonStr);
    IndexPartitionOptions options;
    FromJsonString(options.GetOfflineConfig().mergeConfig, jsonStr);
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDocs =
        "cmd=add,product_id=0,title=hello world,body=taobao,price=10,user_id=1;"
        "cmd=add,product_id=1,title=hello beijing,body=chaoyang,price=20,user_id=2;"
        "cmd=add,product_id=2,title=hello tonghui,body=chaoyang,price=30,user_id=3;"
        "cmd=add,product_id=3,title=hello chaoyang,body=taobao,price=40,user_id=4;"
        "cmd=add,product_id=4,title=bye chaoyang,body=taobao,price=50,user_id=5;"
        "cmd=add,product_id=5,title=bye tonghui,body=taobao,price=60,user_id=6;"
        "cmd=add,product_id=6,title=hello beijing chaoyang,body=taobao,price=70,user_id=7;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title_asc_user_id:hello", "docid=0;docid=1;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title_desc_price:hello", "docid=3;docid=6;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:hello", "docid=3;docid=6;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:taobao", "docid=5;docid=6;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:chaoyang", "docid=4;docid=6;"));


    IndexPartitionReaderPtr partReader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(partReader);
    IndexReaderPtr indexReader = partReader->GetIndexReader();
    CheckTermPosting("hello", "title", "asc_user_id", indexReader, {0,1,3,6});
    CheckTermPosting("hello", "title", "desc_price", indexReader, {0,1,2,3,6});
    CheckTermPosting("taobao", "phrase", "desc_price", indexReader, {0,5,6});
    CheckTermPosting("chaoyang", "phrase", "desc_price", indexReader, {1,2,4,6});
    CheckTermPosting("taobao", "expack", "", indexReader, {0,3,4,5,6}, {2,2,2,2,2});
    CheckTermPosting("taobao", "expack", "desc_price", indexReader, {0,5,6}, {2,2,2});
    CheckTermPosting("hello", "expack", "", indexReader, {0,1,2,3,6}, {1,1,1,1,1});
    CheckTermPosting("hello", "expack", "asc_user_id", indexReader, {0,1,3,6}, {1,1,1,1});

    //inc build
    FileSystemWrapper::AtomicLoad(string(TEST_DATA_PATH) +
                                  "filter_by_meta_truncate_for_sharding_index.json", jsonStr);
    MergeConfig mergeConfig;
    FromJsonString(mergeConfig, jsonStr);
    // not trigger merge by balance_tree, only merge inc segment for truncate
    mergeConfig.mergeStrategyStr = "balance_tree";
    mergeConfig.mergeStrategyParameter.SetLegacyString(
            "base-doc-count=300;max-doc-count=8000;conflict-segment-number=3");
    psm.SetMergeConfig(mergeConfig);
    std::string incMergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), incMergeConfigStr);

    string incDocs =
        "cmd=add,product_id=7,title=hello chaoyang,body=taobao,price=70,user_id=8;"
        "cmd=add,product_id=8,title=hello chaoyang,body=taobao,price=20,user_id=0;"
        "cmd=add,product_id=9,title=hello chaoyang,body=taobao,price=45,user_id=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title_asc_user_id:hello", "docid=0;docid=1;docid=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title_desc_price:hello", "docid=3;docid=6;docid=7;docid=9"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:hello", "docid=3;docid=6;docid=7;docid=9"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:taobao", "docid=5;docid=6;docid=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:chaoyang", "docid=4;docid=6;docid=7"));


    partReader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(partReader);
    indexReader = partReader->GetIndexReader();
    CheckTermPosting("hello", "title", "asc_user_id", indexReader, {0,1,8});
    CheckTermPosting("hello", "title", "desc_price", indexReader, {3,6,7,9});
    CheckTermPosting("hello", "phrase", "desc_price", indexReader, {3,6,7,9});
    CheckTermPosting("taobao", "phrase", "desc_price", indexReader, {5,6,7});
    CheckTermPosting("chaoyang", "phrase", "desc_price", indexReader, {4,6,7});
    CheckTermPosting("taobao", "expack", "", indexReader, {0,3,4,5,6,7,8,9}, {2,2,2,2,2,2,2,2});
    CheckTermPosting("taobao", "expack", "desc_price", indexReader, {5,6,7}, {2,2,2});
    CheckTermPosting("hello", "expack", "", indexReader, {0,1,2,3,6,7,8,9}, {1,1,1,1,1,1,1,1});
    CheckTermPosting("hello", "expack", "asc_user_id", indexReader, {0,1,8}, {1,1,1});

    // rt build, truncate use building segment reader from non truncate sharding index
    string rtDocs =
        "cmd=add,product_id=10,title=hello world bye,body=taobao,price=80,user_id=8,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title_asc_user_id:hello",
                             "docid=0;docid=1;docid=8;docid=10"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title_desc_price:hello",
                             "docid=3;docid=6;docid=7;docid=9;docid=10"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:hello",
                             "docid=3;docid=6;docid=7;docid=9;docid=10"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:taobao",
                             "docid=5;docid=6;docid=7;docid=10"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase_desc_price:chaoyang",
                             "docid=4;docid=6;docid=7"));


    partReader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(partReader);
    indexReader = partReader->GetIndexReader();
    CheckTermPosting("hello", "title", "asc_user_id", indexReader, {0,1,8,10});
    CheckTermPosting("hello", "title", "desc_price", indexReader, {3,6,7,9,10});
    CheckTermPosting("hello", "phrase", "desc_price", indexReader, {3,6,7,9,10});
    CheckTermPosting("taobao", "phrase", "desc_price", indexReader, {5,6,7,10});
    CheckTermPosting("chaoyang", "phrase", "desc_price", indexReader, {4,6,7});
}

void IndexBuilderInteTest::TestShardingIndexBuildAdaptiveBitmapIndex()
{
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            string(TEST_DATA_PATH), "adaptive_bitmap_sharding_example.json");

    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
    options.GetOfflineConfig().buildConfig.enablePackageFile = false;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    string fullDocs =
        "cmd=add,product_id=1,title=hello chaoyang,body=taobao,user_id=1;"
        "cmd=add,product_id=2,title=hello beijing,body=chaoyang,user_id=3;"
        "cmd=add,product_id=3,title=hello chaoyang,body=taobao,user_id=2;"
        "cmd=add,product_id=4,title=hello beijing,body=taobao,user_id=3;"
        "cmd=add,product_id=5,title=bye beijing ,body=taobao,user_id=3;"
        "cmd=add,product_id=6,title=hi chaoyang,body=beijing,user_id=3;";

    // test full build generate adaptive bitmap
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));

    IndexConfigPtr phraseIndexConfig =
        schema->GetIndexSchema()->GetIndexConfig("phrase");
    IndexConfigPtr useridIndexConfig =
        schema->GetIndexSchema()->GetIndexConfig("userid");
    GET_PARTITION_DIRECTORY()->GetDirectory("segment_1_level_0", true)->MountPackageFile(PACKAGE_FILE_PREFIX);
    DirectoryPtr seg1IndexDir =
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_1_level_0/index", true);
    ASSERT_TRUE(seg1IndexDir);
    CheckBitmapOnlyIndexData(seg1IndexDir, phraseIndexConfig);
    CheckBitmapOnlyIndexData(seg1IndexDir, useridIndexConfig);

    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:hello", "docid=0;docid=1;docid=2;docid=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:chaoyang", "docid=0;docid=1;docid=2;docid=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:taobao", "docid=0;docid=2;docid=3;docid=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:bye", "docid=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:hi", "docid=5"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "userid:1", "docid=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "userid:2", "docid=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "userid:3", "docid=1;docid=3;docid=4;docid=5"));

    // test inc build use adaptive dict
    string incDocs =
        "cmd=add,product_id=7,title=hello chaoyang,body=taobao,user_id=1;"
        "cmd=add,product_id=8,title=hi hello beijing,body=chaoyang,user_id=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));

    GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_0", true)->MountPackageFile(PACKAGE_FILE_PREFIX);
    DirectoryPtr seg2IndexDir =
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_0/index", true);
    ASSERT_TRUE(seg2IndexDir);
    CheckBitmapOnlyIndexData(seg2IndexDir, phraseIndexConfig);
    CheckBitmapOnlyIndexData(seg2IndexDir, useridIndexConfig);

    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:hello",
                             "docid=0;docid=1;docid=2;docid=3;docid=6;docid=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:chaoyang",
                             "docid=0;docid=1;docid=2;docid=5;docid=6;docid=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:taobao",
                             "docid=0;docid=2;docid=3;docid=4;docid=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:bye", "docid=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:hi", "docid=5;docid=7"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "userid:1", "docid=0;docid=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "userid:2", "docid=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "userid:3",
                             "docid=1;docid=3;docid=4;docid=5;docid=7"));

    // test rt build use adaptive dict
    string rtDocs =
        "cmd=add,product_id=9,title=hello chaoyang,body=taobao,user_id=3,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:hello",
                             "docid=0;docid=1;docid=2;docid=3;docid=6;docid=7;docid=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:chaoyang",
                             "docid=0;docid=1;docid=2;docid=5;docid=6;docid=7;docid=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "phrase:taobao",
                             "docid=0;docid=2;docid=3;docid=4;docid=6;docid=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "userid:3",
                             "docid=1;docid=3;docid=4;docid=5;docid=7;docid=8"));
}

void IndexBuilderInteTest::TestReadDictInlineIndex()
{
    string fields = "name:string;title:text;pk:string";
    string indexs = "pk_index:primarykey64:pk;"
                    "str_index:string:name:false;"
                    "pack_index:pack:title:false";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(fields, indexs, "", "");

    IndexPartitionOptions options;
    options.GetBuildConfig(true).enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDoc = "cmd=add,pk=0,name=yida,title=staff;"
                     "cmd=add,pk=1,name=pujian,title=abc;"
                     "cmd=add,pk=2,name=shoujing,title=def;"
                     "cmd=add,pk=3,name=yexiao,title=hig;"
                     "cmd=add,pk=4,name=pujian,title=rst;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "str_index:yida", "docid=0;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "str_index:pujian", "docid=1;docid=4;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "str_index:shoujing", "docid=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pack_index:abc", "docid=1;"));


    string rtDoc = "cmd=add,pk=5,name=yida_1,title=staff_11,ts=1;"
                   "cmd=add,pk=6,name=pujian,title=abc_22,ts=2;"
                   "cmd=add,pk=7,name=shoujing,title=def_33,ts=3;"
                   "cmd=add,pk=8,name=yexiao_4,title=hig,ts=4;";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "str_index:yida_1", "docid=5;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "str_index:shoujing", "docid=2;docid=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pack_index:hig", "docid=3;docid=8"));
}

void IndexBuilderInteTest::InnerTestMergeWithAdaptiveBitmap(bool optimize, bool enableArchiveFile)
{
    TearDown();
    SetUp();

    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;numberIndex:number:price;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    SetAdaptiveHighFrequenceDictionary(
            "index2", "DOC_FREQUENCY#3", hp_both, schema);
    SetAdaptiveHighFrequenceDictionary(
            "numberIndex", "DOC_FREQUENCY#3", hp_both, schema);

    string docString = "cmd=add,string1=pk1,string2=hello,price=4;"
                       "cmd=add,string1=pk2,string2=hello,price=4;"
                       "cmd=add,string1=pk3,string2=hello,price=4;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 1;
    options.SetEnablePackageFile(false);
    options.GetMergeConfig().mergeThreadCount = 1;
    options.GetMergeConfig().mImpl->enablePackageFile = true;
    options.GetMergeConfig().mImpl->enableArchiveFile = enableArchiveFile;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    string adaptiveDictPath = PathUtil::JoinPath(
            GET_TEST_DATA_PATH(), ADAPTIVE_DICT_DIR_NAME);
    FileSystemWrapper::AtomicStore(
            PathUtil::JoinPath(adaptiveDictPath, SEGMENT_FILE_LIST), ""); // fake for deploy index meta
    //string adaptiveFilePath = PathUtil::JoinPath(adaptiveDictPath, "index2");

    if (optimize)
    {
        //FileSystemWrapper::AtomicStore(adaptiveFilePath, "");

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
        ArchiveFolderPtr folder(new ArchiveFolder(false));
        folder->Open(adaptiveDictPath);
        FileWrapperPtr file = folder->GetInnerFile("index2", fslib::READ);

        ASSERT_TRUE(file);
        file->Close();
        folder->Close();
        string incDocString = "cmd=add,string1=pk4,string2=hello,price=4;"
                              "cmd=add,string1=pk5,string2=hello,price=5;"
                              "cmd=add,string1=pk6,string2=hello,price=6;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));

        string bitmapPostingFile = PathUtil::JoinPath(GET_TEST_DATA_PATH(),
                "segment_6_level_0/index/index2/bitmap_posting");
        ASSERT_TRUE(FileSystemWrapper::IsExist(bitmapPostingFile));
        bitmapPostingFile = PathUtil::JoinPath(GET_TEST_DATA_PATH(),
                "segment_4_level_0/index/numberIndex/bitmap_posting");
        ASSERT_TRUE(FileSystemWrapper::IsExist(bitmapPostingFile));
        bitmapPostingFile = PathUtil::JoinPath(GET_TEST_DATA_PATH(),
                "segment_6_level_0/index/numberIndex/bitmap_posting");
        ASSERT_FALSE(FileSystemWrapper::IsExist(bitmapPostingFile));
    }
    else
    {
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
        ArchiveFolderPtr folder(new ArchiveFolder(false));
        folder->Open(adaptiveDictPath);
        FileWrapperPtr file = folder->GetInnerFile("index2", fslib::READ);
        ASSERT_FALSE(file);
        folder->Close();
    }
}

// adaptiveRuleStr:
// DOC_FREQUENCY#10, PERCENT#20, INDEX_SIZE#-1
void IndexBuilderInteTest::SetAdaptiveHighFrequenceDictionary(const string& indexName,
        const string& adaptiveRuleStr,
        HighFrequencyTermPostingType type,
        const IndexPartitionSchemaPtr& schema)
{
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    assert(indexSchema != NULL);
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    assert(indexConfig != NULL);

    vector<string> adaptiveParams = StringUtil::split(adaptiveRuleStr, "#");
    assert(adaptiveParams.size() == 2);

    AdaptiveDictionaryConfigPtr dictConfig(new AdaptiveDictionaryConfig("adaptive_rule",
                    adaptiveParams[0], StringUtil::fromString<int32_t>(adaptiveParams[1])));
    AdaptiveDictionarySchemaPtr adaptiveDictSchema(new AdaptiveDictionarySchema);
    adaptiveDictSchema->AddAdaptiveDictionaryConfig(dictConfig);
    schema->SetAdaptiveDictSchema(adaptiveDictSchema);
    indexConfig->SetAdaptiveDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(type);
}

void IndexBuilderInteTest::CheckPackgeFile(const string& segmentDirPath)
{
    FileList fileList;
    FileSystemWrapper::ListDirRecursive(segmentDirPath, fileList);
    ASSERT_EQ((size_t)6, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ(COUNTER_FILE_NAME, fileList[0]);
    ASSERT_EQ(string("deploy_index"), fileList[1]);
    ASSERT_EQ(string("package_file.__data__0"), fileList[2]);
    ASSERT_EQ(string("package_file.__meta__"), fileList[3]);
    ASSERT_EQ(string("segment_file_list"), fileList[4]);
    ASSERT_EQ(string("segment_info"), fileList[5]);
}

void IndexBuilderInteTest::CheckUpdateInfo(const file_system::DirectoryPtr& partDir,
                                           const std::string& updateInfoRelPath,
                                           const std::string& updateInfoStr)
{
    assert(partDir);
    ASSERT_TRUE(partDir->IsExist(updateInfoRelPath));
    string fileContent;
    partDir->Load(updateInfoRelPath, fileContent);
    AttributeUpdateInfo updateInfo;
    FromJsonString(updateInfo, fileContent);

    vector<vector<int> > updateInfoVec;
    StringUtil::fromString(updateInfoStr, updateInfoVec, ":", ",");
    ASSERT_EQ(updateInfoVec.size(), updateInfo.Size());

    AttributeUpdateInfo::Iterator iter = updateInfo.CreateIterator();
    int i = 0;
    while (iter.HasNext())
    {
        SegmentUpdateInfo segUpdateInfo = iter.Next();
        ASSERT_TRUE(updateInfoVec[i].size() == 2);
        ASSERT_EQ((segmentid_t)updateInfoVec[i][0], segUpdateInfo.updateSegId);
        ASSERT_EQ((uint32_t)updateInfoVec[i][1], segUpdateInfo.updateDocCount);
        i++;
    }
}

void IndexBuilderInteTest::TestAdaptiveBitampFailWithMergeDictInlineTerm()
{
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            string(TEST_DATA_PATH), "adaptive_bitmap_sharding_example_dictinline.json");

    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
    options.GetOfflineConfig().buildConfig.enablePackageFile = false;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // uniqterm df=1
    string fullDocs =
        "cmd=add,product_id=1,title=hello chaoyang uniqterm,body=taobao,user_id=1;"
        "cmd=add,product_id=2,title=hello beijing,body=chaoyang,user_id=3;"
        "cmd=add,product_id=3,title=hello chaoyang,body=taobao,user_id=2;"
        "cmd=add,product_id=4,title=hello beijing,body=taobao,user_id=3;"
        "cmd=add,product_id=5,title=bye beijing ,body=taobao,user_id=3;"
        "cmd=add,product_id=6,title=hi chaoyang,body=beijing,user_id=3;";

    // test full build generate adaptive bitmap
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
}

void IndexBuilderInteTest::TestIncTruncateByMetaWithDictInlineTerm()
{
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            string(TEST_DATA_PATH), "simple_truncate_example_dictinline.json");

    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("title");
    ASSERT_TRUE(indexConfig);
    indexConfig->SetOptionFlag(indexConfig->GetOptionFlag() | of_term_payload);

    string jsonStr;
    FileSystemWrapper::AtomicLoad(string(TEST_DATA_PATH) +
                                  "simple_truncate_for_sharding_index.json", jsonStr);
    IndexPartitionOptions options;
    FromJsonString(options.GetOfflineConfig().mergeConfig, jsonStr);
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    // full build
    string fullDocs =
        "cmd=add,product_id=0,title=hello world,body=taobao,price=10,user_id=1;"
        "cmd=add,product_id=1,title=hello beijing,body=chaoyang,price=20,user_id=2;"
        "cmd=add,product_id=2,title=hello tonghui,body=chaoyang,price=30,user_id=3;"
        "cmd=add,product_id=3,title=hello chaoyang,body=taobao,price=40,user_id=4;"
        "cmd=add,product_id=4,title=bye chaoyang,body=taobao,price=50,user_id=5;"
        "cmd=add,product_id=5,title=bye tonghui,body=taobao,price=60,user_id=6;"
        "cmd=add,product_id=6,title=hello beijing chaoyang,body=taobao,price=70,user_id=7;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
    // inc build
    FileSystemWrapper::AtomicLoad(string(TEST_DATA_PATH) +
                                  "filter_by_meta_truncate_for_sharding_index.json", jsonStr);
    MergeConfig mergeConfig;
    FromJsonString(mergeConfig, jsonStr);
    // not trigger merge by balance_tree, only merge inc segment for truncate
    mergeConfig.mergeStrategyStr = "balance_tree";
    mergeConfig.mergeStrategyParameter.SetLegacyString(
            "base-doc-count=300;max-doc-count=8000;conflict-segment-number=3");
    psm.SetMergeConfig(mergeConfig);
    psm.SetTermPayloadMap({{"hello", (termpayload_t)22}});
    string incDocs =
        "cmd=add,product_id=8,title=hello chaoyang,body=taobao,price=20,user_id=0;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title_asc_user_id:hello", "docid=0;docid=1;docid=7"));

    IndexPartitionReaderPtr partReader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(partReader);
    IndexReaderPtr indexReader = partReader->GetIndexReader();
    CheckTermPosting("hello", "title", "asc_user_id", indexReader,
                     {0,1,7}, {}, (termpayload_t)22);
}

void IndexBuilderInteTest::TestBuildAndMergeWithCompressPatch()
{
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::LoadSchema(string(TEST_DATA_PATH)+"pack_attribute/",
                                  "main_schema_with_pack_patch_compress.json");
    IndexPartitionSchemaPtr subSchema =
        SchemaAdapter::LoadSchema(string(TEST_DATA_PATH)+"pack_attribute/",
                                  "sub_schema_with_pack_patch_compress.json");
    schema->SetSubIndexPartitionSchema(subSchema);

    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    options.GetMergeConfig().mergeStrategyStr = "specific_segments";
    options.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
            "merge_segments=2,3");

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    string fullDocs =
        "cmd=add,int8_single=0,int8_multi=0 1,str_single=pk,"
        "sub_int8_single=36^37,sub_int8_multi=4 5^6 7,sub_str_single=subpk1^subpk2;"
        "cmd=add,int8_single=0,int8_multi=0 1,str_single=pk1,"
        "sub_int8_single=3^6,sub_int8_multi=4 5^2 4,sub_str_single=subpk5^subpk6;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));

    string incDocs1 = "cmd=update_field,int8_single=10,int8_multi=10 11,str_single=pk,"
                      "sub_int8_single=39,sub_int8_multi=4 5 7,sub_str_single=subpk1;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "", ""));
    DirectoryPtr mainAttrDir =
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_0/attribute", true);
    DirectoryPtr subAttrDir =
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_2_level_0/sub_segment/attribute", true);
    string incDocs2 = "cmd=update_field,int8_single=20,int8_multi=30 32,str_single=pk1,"
                      "sub_int8_single=39,sub_int8_multi=4 5 7,sub_str_single=subpk5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
}

void IndexBuilderInteTest::TestEnablePatchCompress()
{
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        string field = "string1:string;"
            "p0:string:false:true:patch_compress;"
            "p1:double:false:false:patch_compress;"
            "p2:uint32:false:false:patch_compress;"
            "p3:uint32:true:true:patch_compress";
        string index = "pk:primarykey64:string1";
        string attribute = "string1;p0;p1;packAttr:p2,p3";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

        string fullDoc = "cmd=add,string1=s0,p0=hi0,p1=0.4,p2=6,p3=0 1;"
            "cmd=add,string1=s1,p0=hi1,p1=1.4,p2=7,p3=1 2;"
            "cmd=add,string1=s2,p0=hi2,p1=2.4,p2=8,p3=2 3;"
            "cmd=add,string1=s3,p0=hi3,p1=3.4,p2=9,p3=3 4;";

        PartitionStateMachine psm;
        IndexPartitionOptions options;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "", ""));

        string incDoc0 = "cmd=update_field,string1=s0,p0=hi10,p1=0.1,p2=36,p3=1 0;"
            "cmd=update_field,string1=s1,p0=hi11,p1=0.2,p2=37,p3=2 1;";
        string incDoc1 = "cmd=update_field,string1=s2,p0=hi12,p1=0.3,p2=38,p3=3 2;"
            "cmd=update_field,string1=s3,p0=hi13,p1=0.4,p2=39,p3=4 3;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc0, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc1, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s0", "p0=hi10,p1=0.1,p2=36,p3=1 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s1", "p0=hi11,p1=0.2,p2=37,p3=2 1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s2", "p0=hi12,p1=0.3,p2=38,p3=3 2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s3", "p0=hi13,p1=0.4,p2=39,p3=4 3"));

        string rtDoc = "cmd=update_field,string1=s0,p0=ha0,p1=1.1,p2=16,p3=1 0,ts=5;"
            "cmd=update_field,string1=s1,p0=ha1,p1=1.2,p2=17,p3=2 1,ts=10;"
            "cmd=update_field,string1=s2,p0=ha2,p1=1.3,p2=18,p3=3 2,ts=15;"
            "cmd=update_field,string1=s3,p0=ha3,p1=1.4,p2=19,p3=4 3,ts=20;";

        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s0", "p0=ha0,p1=1.1,p2=16,p3=1 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s1", "p0=ha1,p1=1.2,p2=17,p3=2 1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s2", "p0=ha2,p1=1.3,p2=18,p3=3 2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s3", "p0=ha3,p1=1.4,p2=19,p3=4 3"));

        string incDoc2 = "cmd=update_field,string1=s0,p0=ho4,p1=3.5,p2=30,p3=3 6,ts=6;"
            "cmd=update_field,string1=s1,p0=ho5,p1=3.6,p2=31,p3=3 7,ts=9;"
            "cmd=update_field,string1=s2,p0=ho6,p1=3.7,p2=32,p3=3 8,ts=10;"
            "cmd=update_field,string1=s3,p0=ho7,p1=3.8,p2=33,p3=3 9,ts=10;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc2, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s0", "p0=ho4,p1=3.5,p2=30,p3=3 6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s1", "p0=ha1,p1=1.2,p2=17,p3=2 1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s2", "p0=ha2,p1=1.3,p2=18,p3=3 2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s3", "p0=ha3,p1=1.4,p2=19,p3=4 3"));

    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        string field = "string1:string;"
            "p0:string:false:true:patch_compress;"
            "p1:double:false:false:patch_compress;"
            "p2:uint32:false:false:patch_compress;"
            "p3:uint32:true:true:patch_compress";
        string index = "pk:primarykey64:string1";
        string attribute = "string1;p0;p1;packAttr:p2,p3";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

        string fullDoc = "cmd=add,string1=s0,p0=hi0,p1=0.4,p2=6,p3=0 1;"
            "cmd=add,string1=s1,p0=hi1,p1=1.4,p2=7,p3=1 2;"
            "cmd=add,string1=s2,p0=hi2,p1=2.4,p2=8,p3=2 3;"
            "cmd=add,string1=s3,p0=hi3,p1=3.4,p2=9,p3=3 4;";

        PartitionStateMachine psm;
        IndexPartitionOptions options;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "", ""));

        string incDoc0 = "cmd=update_field,string1=s0,p0=hi10,p1=0.1,p2=36,p3=1 0;"
            "cmd=update_field,string1=s1,p0=hi11,p1=0.2,p2=37,p3=2 1;";
        string incDoc1 = "cmd=update_field,string1=s2,p0=hi12,p1=0.3,p2=38,p3=3 2;"
            "cmd=update_field,string1=s3,p0=hi13,p1=0.4,p2=39,p3=4 3;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc0, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc1, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s0", "p0=hi10,p1=0.1,p2=36,p3=1 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s1", "p0=hi11,p1=0.2,p2=37,p3=2 1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s2", "p0=hi12,p1=0.3,p2=38,p3=3 2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s3", "p0=hi13,p1=0.4,p2=39,p3=4 3"));

        string rtDoc = "cmd=update_field,string1=s0,p0=ha0,p1=1.1,p2=16,p3=1 0,ts=5;"
            "cmd=update_field,string1=s1,p0=ha1,p1=1.2,p2=17,p3=2 1,ts=10;"
            "cmd=update_field,string1=s2,p0=ha2,p1=1.3,p2=18,p3=3 2,ts=15;"
            "cmd=update_field,string1=s3,p0=ha3,p1=1.4,p2=19,p3=4 3,ts=20;";

        ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s0", "p0=ha0,p1=1.1,p2=16,p3=1 0"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s1", "p0=ha1,p1=1.2,p2=17,p3=2 1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s2", "p0=ha2,p1=1.3,p2=18,p3=3 2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s3", "p0=ha3,p1=1.4,p2=19,p3=4 3"));

        string incDoc2 = "cmd=update_field,string1=s0,p0=ho4,p1=3.5,p2=30,p3=3 6,ts=6;"
            "cmd=update_field,string1=s1,p0=ho5,p1=3.6,p2=31,p3=3 7,ts=9;"
            "cmd=update_field,string1=s2,p0=ho6,p1=3.7,p2=32,p3=3 8,ts=10;"
            "cmd=update_field,string1=s3,p0=ho7,p1=3.8,p2=33,p3=3 9,ts=10;";

        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc2, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s0", "p0=ho4,p1=3.5,p2=30,p3=3 6"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s1", "p0=ho5,p1=3.6,p2=31,p3=3 7"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s2", "p0=ha2,p1=1.3,p2=18,p3=3 2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s3", "p0=ha3,p1=1.4,p2=19,p3=4 3"));
    }
}

void IndexBuilderInteTest::TestTruncateIndex()
{
    // prepare
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            string(TEST_DATA_PATH), "truncate_example.json");
    string jsonStr;
    FileSystemWrapper::AtomicLoad(string(TEST_DATA_PATH) +
                                  "simple_truncate_for_sharding_index.json", jsonStr);
    IndexPartitionOptions options;
    FromJsonString(options.GetOfflineConfig().mergeConfig, jsonStr);
    options.GetOfflineConfig().buildConfig.enablePackageFile = false;
    options.GetOfflineConfig().buildConfig.keepVersionCount = 10;
    options.GetMergeConfig().mImpl->enableArchiveFile = true;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDocs =
        "cmd=add,product_id=0,title=hello world,body=taobao,price=10,user_id=1;"
        "cmd=add,product_id=1,title=hello beijing,body=chaoyang,price=20,user_id=2;"
        "cmd=add,product_id=2,title=hello tonghui,body=chaoyang,price=30,user_id=3;"
        "cmd=add,product_id=3,title=hello chaoyang,body=taobao,price=40,user_id=4;"
        "cmd=add,product_id=4,title=bye chaoyang,body=taobao,price=50,user_id=5;"
        "cmd=add,product_id=5,title=bye tonghui,body=taobao,price=60,user_id=6;"
        "cmd=add,product_id=6,title=hello beijing chaoyang,body=taobao,price=70,user_id=7;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));

    IndexPartitionReaderPtr partReader = psm.GetIndexPartition()->GetReader();
    IndexReaderPtr indexReader = partReader->GetIndexReader();
    CheckTermPosting("hello", "title", "asc_user_id", indexReader, {0,1});
    CheckTermPosting("hello", "title", "desc_price", indexReader, {3,6});
    CheckTermPosting("taobao", "phrase_pos", "", indexReader, {0,3,4,5,6}, {2,2,2,2,2});
    CheckTermPosting("taobao", "phrase_pos", "desc_price", indexReader, {5,6}, {2,2});
    CheckTermPosting("chaoyang", "phrase_pos", "desc_price", indexReader, {4,6}, {1,1});
    CheckTermPosting("taobao", "phrase", "", indexReader, {0,3,4,5,6}, {2,2,2,2,2});
    CheckTermPosting("taobao", "phrase", "desc_price", indexReader, {5,6}, {2,2});
    CheckTermPosting("chaoyang", "phrase", "desc_price", indexReader, {4,6}, {1,1});

    // inc build
    FileSystemWrapper::AtomicLoad(string(TEST_DATA_PATH) +
                                  "filter_by_meta_truncate_for_sharding_index.json", jsonStr);
    MergeConfig mergeConfig;
    FromJsonString(mergeConfig, jsonStr);
    // not trigger merge by balance_tree, only merge inc segment for truncate
    mergeConfig.mergeStrategyStr = "balance_tree";
    mergeConfig.mergeStrategyParameter.SetLegacyString(
            "base-doc-count=300;max-doc-count=8000;conflict-segment-number=3");
    psm.SetMergeConfig(mergeConfig);

    string incDocs =
        "cmd=add,product_id=7,title=hello chaoyang,body=taobao,price=70,user_id=8;"
        "cmd=add,product_id=8,title=hello chaoyang,body=taobao,price=20,user_id=0;"
        "cmd=add,product_id=9,title=hello chaoyang,body=taobao,price=45,user_id=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "", ""));

    partReader = psm.GetIndexPartition()->GetReader();
    indexReader = partReader->GetIndexReader();
    CheckTermPosting("hello", "title", "asc_user_id", indexReader, {0,1,8});
    CheckTermPosting("hello", "title", "desc_price", indexReader, {3,6,7,9});
    CheckTermPosting("hello", "phrase", "desc_price", indexReader, {3,6,7,9}, {1,1,1,1});
    CheckTermPosting("taobao", "phrase", "desc_price", indexReader, {5,6,7}, {2,2,2});
    CheckTermPosting("chaoyang", "phrase", "desc_price", indexReader, {4,6,7}, {1,1,1});

    // rt build, truncate use building segment reader
    string rtDocs =
        "cmd=add,product_id=10,title=hello world bye,body=taobao,price=80,user_id=8,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));

    partReader = psm.GetIndexPartition()->GetReader();
    indexReader = partReader->GetIndexReader();
    CheckTermPosting("hello", "title", "asc_user_id", indexReader, {0,1,8,10});
    CheckTermPosting("hello", "title", "desc_price", indexReader, {3,6,7,9,10});
    CheckTermPosting("hello", "phrase", "desc_price", indexReader, {3,6,7,9,10}, {1,1,1,1,1});
    CheckTermPosting("taobao", "phrase", "desc_price", indexReader, {5,6,7,10}, {2,2,2,2});
    CheckTermPosting("chaoyang", "phrase", "desc_price", indexReader, {4,6,7}, {1,1,1});
}

void IndexBuilderInteTest::TestDictInlineIndexWithTermPayload()
{
    string field = "string1:string;string2:string";
    string index = "index2:string:string2;pk:primarykey64:string1";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "", "");

    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index2");
    ASSERT_TRUE(indexConfig);
    indexConfig->SetOptionFlag(indexConfig->GetOptionFlag() | of_term_payload);

    string docString = "cmd=add,string1=pk1,string2=hello;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.SetEnablePackageFile(false);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    psm.SetTermPayloadMap({{"hello", (termpayload_t)22}});
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    IndexPartitionReaderPtr partReader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(partReader);
    IndexReaderPtr indexReader = partReader->GetIndexReader();
    CheckTermPosting("hello", "index2", "", indexReader, {0}, {}, (termpayload_t)22);
}

void IndexBuilderInteTest::CheckTermPosting(
        const std::string& word, const std::string& indexName,
        const std::string& truncateName,
        const IndexReaderPtr& indexReader, const vector<docid_t>& expectDocIds,
        const std::vector<int32_t>& expectFieldMaps,
        int32_t expectTermpayload)
{
    mem_pool::Pool pool;
    common::Term term(word, indexName, truncateName);
    PostingIterator *iter = indexReader->Lookup(
            term, 1000, pt_default, &pool);
    ASSERT_TRUE(iter);

    index::TermMeta* termMeta = iter->GetTermMeta();
    index::TermMeta* truncateTermMeta = iter->GetTruncateTermMeta();
    ASSERT_TRUE(termMeta);
    ASSERT_TRUE(truncateTermMeta);

    if (expectTermpayload != -1)
    {
        ASSERT_EQ((termpayload_t)expectTermpayload, termMeta->GetPayload())
            << "term payload not equal. Case: "
            << "index:" << indexName + "_" + truncateName<< ", word:" << word;

        ASSERT_EQ((termpayload_t)expectTermpayload, truncateTermMeta->GetPayload())
            << "term payload not equal. Case: "
            << "index:" << indexName + "_" + truncateName<< ", word:" << word;
    }

    docid_t docId = INVALID_DOCID;
    vector<docid_t> docIdVec;
    vector<int32_t> fieldMapVec;

    TermMatchData tmd;
    while((docId = iter->SeekDoc(docId)) != INVALID_DOCID)
    {
        docIdVec.push_back(docId);
        if (!expectFieldMaps.empty())
        {
            iter->Unpack(tmd);
            fieldMapVec.push_back((int32_t)tmd.GetFieldMap());
        }
    }
    ASSERT_EQ(expectDocIds, docIdVec)
        << "docid not equal. Case: "
        << "index:" << indexName + "_" + truncateName<< ", word:" << word;
    ASSERT_EQ(expectFieldMaps, fieldMapVec)
        << "fieldmap not equal. Case: "
        << "index:" << indexName + "_" + truncateName<< ", word:" << word;
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, iter);
}

void IndexBuilderInteTest::TestOfflineRecover()
{
    string field = "string1:string;"
                   "p0:string:false:true;"
                   "p1:double:false:false;"
                   "p2:uint32:false:false;"
                   "p3:uint32:true:true;";
    string index = "pk:primarykey64:string1";
    string attribute = "string1;p0;p1;packAttr:p2,p3";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string fullDoc = "cmd=add,string1=s0,p0=hi0,p1=0.4,p2=6,p3=0 1;"
                     "cmd=add,string1=s1,p0=hi1,p1=1.4,p2=7,p3=1 2;"
                     "cmd=add,string1=s2,p0=hi2,p1=2.4,p2=8,p3=2 3;"
                     "cmd=add,string1=s3,p0=hi3,p1=3.4,p2=9,p3=3 4;";

    string incDoc = "cmd=add,string1=s4,p0=hi4,p1=4.4,p2=10,p3=4 5;";

    {
        // full build
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "pk:s2", "docid=2"));

        GET_PARTITION_DIRECTORY()->RemoveFile("version.0");
    }
    {
        // recover build
        PartitionStateMachine psm;
        BuildConfig buildConfig;
        MergeConfig mergeConfig;

        char* buffer = new char[sizeof(OfflineConfig)];
        memset(buffer, 0, sizeof(OfflineConfig));
        OfflineConfig* offlineConfig =
            new (buffer) OfflineConfig(buildConfig, mergeConfig);

        IndexPartitionOptions options;
        options.SetOfflineConfig(*offlineConfig);
        ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "pk:s2", "docid=2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:s4", "docid=4"));
        offlineConfig->~OfflineConfig();
        delete [] buffer;
    }
}

void IndexBuilderInteTest::TestUpdateDocFailWhenFlushRtIndex()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5;"
                       "cmd=add,string1=hello,price=6;";
    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 1;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:hello", "docid=0,price=6"));

    string rtDocs = "cmd=update_field,string1=hello,price=10,ts=10;";
    // rtDocs not success
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "pk:hello", "docid=0,price=6"));
}

void IndexBuilderInteTest::TestAutoAdd2UpdateWhenFlushRtIndex()
{
    string field = "pk:string;price:uint32;info:string:false:true";
    string index = "pk:primarykey64:pk";
    string attribute = "price;info;pk";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    psm.Transfer(BUILD_FULL, "", "", "");

    string rtDocs = "cmd=add,pk=pk1,price=123,info=abc,ts=1;"
                    "cmd=add,pk=pk1,price=234,info=abcd,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT,
                             rtDocs, "pk:pk1", "docid=1,price=234,info=abcd"));

    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    IndexPartitionReaderPtr reader = indexPart->GetReader();
    docid_t totalDocCount = reader->GetPartitionInfo()->GetPartitionMetrics().totalDocCount;
    ASSERT_EQ((docid_t)2, totalDocCount);
}

void IndexBuilderInteTest::TestOnlineIntervalDump()
{
    string field = "pk:string;price:uint32;info:string:false:true";
    string index = "pk:primarykey64:pk";
    string attribute = "price;info;pk";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().maxRealtimeDumpInterval = 5; // 5s

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    psm.Transfer(BUILD_FULL, "", "", "");

    string rtDocs = "cmd=add,pk=pk1,price=123,info=abc,ts=1;"
                    "cmd=add,pk=pk1,price=234,info=abcd,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "pk:pk1", "docid=1,price=234,info=abcd"));

    string path = "/rt_index_partition/segment_1073741824_level_0/segment_info";
    ASSERT_FALSE(GET_PARTITION_DIRECTORY()->IsExist(path));
    sleep(10);
    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist(path));
}

void IndexBuilderInteTest::TestBuildRecoverWithNonDefaultSchemaVersion()
{
    string field = "pk:string;price:uint32;info:string:false:true";
    string index = "pk:primarykey64:pk";
    string attribute = "price;info;pk";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->SetSchemaVersionId(1);

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    psm.Transfer(BUILD_FULL, "", "", "");

    Version version;
    VersionLoader::GetVersion(GET_TEST_DATA_PATH(), version, 0);
    ASSERT_EQ((schemavid_t)1, version.GetSchemaVersionId());

    GET_PARTITION_DIRECTORY()->RemoveFile("version.0");

    string incDoc = "cmd=add,pk=pk1,price=123,info=abc,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "pk:pk1", "docid=0,price=123,info=abc"));

    Version recoverVersion;
    VersionLoader::GetVersion(GET_TEST_DATA_PATH(), recoverVersion, INVALID_VERSION);
    ASSERT_EQ((schemavid_t)1, recoverVersion.GetSchemaVersionId());
}

void IndexBuilderInteTest::TestMergeMultiSegment()
{
    string docString = "cmd=add,string1=hello1,price=4,string2=string2;"
                       "cmd=add,string1=hello2,price=5;string2=string2"
                       "cmd=add,string1=hello3,price=6;string2=string2";
    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 2;
    options.GetMergeConfig().mergeStrategyStr = "optimize";
    options.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
        "max-doc-count=30;after-merge-max-segment-count=1");
    std::string mergeConfigStr
        = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "pk:hello3",
                             "docid=1,price=6"));
}

void IndexBuilderInteTest::TestMergeMutiSegmentTypeString()
{
    // string
    string docString = "cmd=add,string1=hello1,price=4,string2=string2;"
                       "cmd=add,string1=hello2,price=5;string2=string2"
                       "cmd=add,string1=hello3,price=6;string2=string3"
                       "cmd=add,string1=hello4,price=7;string2=string3"
                       "cmd=add,string1=hello4,price=8;string2=string4";
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1;"
                   "numberIndex:number:price;";
    string attribute = "string1;price";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    IndexPartitionOptions options;
    MakeConfig(2, options, 30);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:hello4", "docid=2,price=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:8", "docid=2"));
    FileSystemWrapper::DeleteDir(GET_TEST_DATA_PATH());
    FileSystemWrapper::MkDir(GET_TEST_DATA_PATH());

    //sort merge
    PartitionStateMachine psm1;
    MakeConfig(3, options, 30, true, "price");
    ASSERT_TRUE(psm1.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm1.Transfer(BUILD_FULL, docString, "pk:hello1", "docid=0,price=4"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(GET_TEST_DATA_PATH() + "/segment_3_level_0/"));

    ASSERT_TRUE(psm1.Transfer(QUERY, "", "pk:hello2", "docid=1,price=5"));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "numberIndex:6", "docid=3"));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "pk:hello4", "docid=2,price=8"));
}

void IndexBuilderInteTest::TestMergeMutiSegmentTypePack()
{
    string field = "text1:text;"
                   "string1:string;"
                   "long1:uint32;"
                   "string2:string;";
    string index = "pack1:pack:text1;"
                   "pk:primarykey64:string1;"
                   "index2:string:string2;";
    string attr = "long1;";
    string summary = "text1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    string docString = "cmd=add,string1=hello1,long1=1,text1=text1,string2=world1;"
                       "cmd=add,string1=hello2,long1=2,text1=text2;"
                       "cmd=add,string1=hello3,long1=3,text1=text3,string2=world3;"
                       "cmd=add,string1=hello3,long1=4,text1=text2;"
                       "cmd=add,string1=hello4,long1=5,text1=text2;";
    string incDocString = "cmd=add,string1=hello5,long1=6,text1=text2;"
                          "cmd=add,string1=hello1,long1=6,text1=text2;";
    {
        IndexPartitionOptions options;
        MakeConfig(3, options, 30);
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:hello3", "docid=1,long1=4"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pack1:text1", "docid=0;"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:world1", "docid=0,long1=1,text1=text1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:world3", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "pk:hello2", "docid=0,long1=2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello1", "docid=4,long1=6"));
    }
    FileSystemWrapper::DeleteDir(GET_TEST_DATA_PATH());
    FileSystemWrapper::MkDir(GET_TEST_DATA_PATH());
    // sort
    {
        IndexPartitionOptions options;
        MakeConfig(3, options, 30, true, "long1");
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:hello3", "docid=1,long1=4"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pack1:text1", "docid=0;"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:world1", "docid=0,long1=1,text1=text1"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:world3", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocString, "pk:hello2", "docid=0,long1=2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello1", "docid=4,long1=6"));
    }
}

void IndexBuilderInteTest::TestMergeMutiSegmentTypeRange()
{
    string field = "price:int64;pk:string";
    string index = "price:range:price;pk:primarykey64:pk";
    string attribute = "pk;price";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    string docString = "cmd=add,price=100,pk=a,ts=1;"
                       "cmd=add,price=128,pk=b,ts=1;"
                       "cmd=add,price=3445205234,pk=c,ts=1;"
                       "cmd=add,price=15160963460000000,pk=d,ts=1";
    IndexPartitionOptions options;
    MakeConfig(3, options, 30);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "price:[0,3445205234]",
                             "docid=0,price=100;docid=2,price=128;docid=3,price=3445205234"));
}

void IndexBuilderInteTest::TestMergeMutiSegmentTypeRangeWithEmptyOutput()
{
    string field = "price:int64;pk:string";
    string index = "price:range:price;pk:primarykey64:pk";
    string attribute = "pk;price";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    string docString = "cmd=add,price=100,pk=a,ts=1;"
                       "cmd=add,price=128,pk=b,ts=1;"
                       "cmd=add,price=3445205234,pk=c,ts=1;"
                       "cmd=add,price=15160963460000000,pk=d,ts=1";
    IndexPartitionOptions options;
    MakeConfig(5, options, 30);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "price:[0,3445205234]",
                             "docid=0,price=100;docid=1,price=128;docid=2,price=3445205234"));
}

void IndexBuilderInteTest::TestMergeMutiSegmentTypeRangeWithEmptyOutputWithSubDoc()
{
    string field = "price:int64;pk:string";
    string index = "price:range:price;pk:primarykey64:pk";
    string attribute = "pk;price";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    config::IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(subSchema,
            "sub_price:int64;sub_url:string",
            "sub_price:range:sub_price;sub_pk:primarykey64:sub_url",
            "sub_price", "");

    schema->SetSubIndexPartitionSchema(subSchema);

    string docString = "cmd=add,price=100,pk=a,ts=1,sub_url=1^2,sub_price=100^128;"
                       "cmd=add,price=128,pk=b,ts=1,sub_url=3;"
                       "cmd=add,price=3445205234,pk=c,ts=1;"
                       "cmd=add,price=15160963460000000,pk=d,ts=1,sub_url=3^4,sub_price=3445205234^344520523400";
    IndexPartitionOptions options;
    MakeConfig(5, options, 30);
    options.GetMergeConfig().SetEnablePackageFile(false);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:b",
                             "docid=1,price=128"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_price:[0,3445205234]", "docid=0,sub_price=100;docid=1,sub_price=128;docid=2,sub_price=3445205234"));
}

void IndexBuilderInteTest::MakeConfig(int outputSegmentCount, config::IndexPartitionOptions& options,
                                      int buildMaxDocCount, bool sort, std::string
                                      sortField, SortPattern sortPattern)
{

    config::IndexPartitionOptions optionsOther;
    options = optionsOther;
    options.GetBuildConfig().maxDocCount = buildMaxDocCount;
    options.GetMergeConfig().mergeStrategyStr = "optimize";
    options.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
        "max-doc-count=30;after-merge-max-segment-count=1");
    std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\""
        + StringUtil::toString(outputSegmentCount) + "\"}}";
    autil::legacy::FromJsonString(
        options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
    if (sort)
    {
        PartitionMeta partitionMeta;
        partitionMeta.AddSortDescription(sortField, sortPattern);
        partitionMeta.Store(GET_PARTITION_DIRECTORY());
    }
}

void IndexBuilderInteTest::TestFixLengthStringAttribute()
{
    string field = "key:string;payload:string:false:true::4;price:uint32";
    string index = "pk:primarykey64:key";
    string attribute = "payload;price";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;

    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeStrategyStr = SPECIFIC_SEGMENTS_MERGE_STRATEGY_STR;
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=4,5");

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDoc = "cmd=add,key=0,payload=0123,price=0;"
                     "cmd=add,key=1,payload=1234,price=1;"
                     "cmd=add,key=2,payload=2345,price=2;"
                     "cmd=add,key=3,payload=3456,price=3;"
                     "cmd=add,key=4,payload=4567,price=4;"
                     "cmd=add,key=5,payload=5678,price=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "payload=0123,price=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "payload=2345,price=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", "payload=5678,price=5"));

    // inc build, not merge
    string incDoc = "cmd=add,key=0,payload=0321,price=2,ts=1;"
                    "cmd=update_field,key=1,payload=1432,ts=1;"
                    "cmd=update_field,key=4,payload=4765,ts=1;"
                    "cmd=add,key=6,payload=67890,price=6,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "payload=0321,price=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "payload=1432,price=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "payload=4765,price=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:6", "payload=6789,price=6"));

    // inc build, optimize merge
    string incDoc2 = "cmd=add,key=7,payload=78910,price=7,ts=2;"
                     "cmd=update_field,key=4,payload=4767,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc2, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "payload=0321,price=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "payload=4767,price=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:7", "payload=7891,price=7"));

    // rt build
    string rtDoc = "cmd=add,key=8,payload=8 9 10 11,price=8,ts=3;"
                   "cmd=update_field,key=6,payload=6789,ts=4;"
                   "cmd=update_field,key=8,payload=811109,ts=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:8", "payload=8111,price=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:6", "payload=6789,price=6"));
}

void IndexBuilderInteTest::TestFixLengthMultiValueAttribute()
{
    InnerTestFixLengthMultiValueAttribute(false, "uint8", "");
    InnerTestFixLengthMultiValueAttribute(false, "int8", "uniq");

    InnerTestFixLengthMultiValueAttribute(true, "uint16", "");
    InnerTestFixLengthMultiValueAttribute(false, "int16", "equal");

    InnerTestFixLengthMultiValueAttribute(true, "uint32", "");
    InnerTestFixLengthMultiValueAttribute(false, "int32", "uniq|equal");

    InnerTestFixLengthMultiValueAttribute(false, "uint64", "");
    InnerTestFixLengthMultiValueAttribute(true, "int64", "");

    InnerTestFixLengthMultiValueAttribute(true, "double", "");
    InnerTestFixLengthMultiValueAttribute(false, "double", "equal");
}

void IndexBuilderInteTest::TestFixLengthEncodeFloatAttribute()
{
    InnerTestFixLengthMultiValueAttribute(false, "float", "");
    InnerTestFixLengthMultiValueAttribute(true, "float", "");

    InnerTestFixLengthMultiValueAttribute(false, "float", "fp16");
    InnerTestFixLengthMultiValueAttribute(true, "float", "fp16");

    InnerTestFixLengthMultiValueAttribute(false, "float", "int8#127");
    InnerTestFixLengthMultiValueAttribute(true, "float", "int8#127");

    InnerTestFixLengthMultiValueAttribute(false, "float", "block_fp");
    InnerTestFixLengthMultiValueAttribute(true, "float", "block_fp");

    InnerTestFixLengthMultiValueAttribute(false, "float", "fp16|uniq");
    InnerTestFixLengthMultiValueAttribute(true, "float", "fp16|uniq|equal");

    InnerTestFixLengthMultiValueAttribute(false, "float", "block_fp|uniq");
    InnerTestFixLengthMultiValueAttribute(true, "float", "block_fp|equal");

    InnerTestFixLengthMultiValueAttribute(false, "float", "int8#127|uniq");
    InnerTestFixLengthMultiValueAttribute(true, "float", "int8#127|equal");
}

void IndexBuilderInteTest::TestSolidPathGetDirectory()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello,price=5;"
                       "cmd=add,string1=hello,price=6;";
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 1;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "pk:hello",
                             "docid=0,price=6"));

    IndexlibFileSystemPtr fileSystem =
        FileSystemFactory::Create(GET_TEST_DATA_PATH(), "", options, NULL);
    SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(fileSystem);

    SegmentData segData = segDir->GetSegmentData(segDir->GetVersion().GetLastSegment());
    DirectoryPtr segDirectory = segData.GetDirectory();
    FileSystemWrapper::DeleteDir(segDirectory->GetPath());

    DirectoryPtr targetDir = segDirectory->GetDirectory("index", true);
    ASSERT_TRUE(targetDir);

    ASSERT_FALSE(FileSystemWrapper::IsExist(targetDir->GetPath()));

    targetDir = segDirectory->GetDirectory("attribute/price", true);
    ASSERT_TRUE(targetDir);
    ASSERT_FALSE(FileSystemWrapper::IsExist(targetDir->GetPath()));
}

void IndexBuilderInteTest::InnerTestFixLengthMultiValueAttribute(
        bool isPackAttr, const string& fieldType, const string& encodeStr)
{
    TearDown();
    SetUp();

    string fieldInfo = "payload:" + fieldType + ":true:true:" + encodeStr + ":4";
    string field = "key:string;" + fieldInfo + ";price:uint32";
    string index = "pk:primarykey64:key";
    string attribute = "payload;price";
    if (isPackAttr)
    {
        attribute = "packAttr:payload,price;";
    }

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;

    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeStrategyStr = SPECIFIC_SEGMENTS_MERGE_STRATEGY_STR;
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=4,5");

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDoc = "cmd=add,key=0,payload=0 1 2 3,price=0;"
                     "cmd=add,key=1,payload=1 2 3 4,price=1;"
                     "cmd=add,key=2,payload=2 3 4 5,price=2;"
                     "cmd=add,key=3,payload=3 4 5 6,price=3;"
                     "cmd=add,key=4,payload=4 5 6 7,price=4;"
                     "cmd=add,key=5,payload=5 6 7 8,price=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "payload=0 1 2 3,price=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:2", "payload=2 3 4 5,price=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:5", "payload=5 6 7 8,price=5"));

    // inc build, not merge
    string incDoc = "cmd=add,key=0,payload=0 3 2 1,price=2,ts=1;"
                    "cmd=update_field,key=1,payload=1 4 3 2,ts=1;"
                    "cmd=update_field,key=4,payload=4 7 6 5,ts=1;"
                    "cmd=add,key=6,payload=6 7 8,price=6,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "payload=0 3 2 1,price=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "payload=1 4 3 2,price=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "payload=4 7 6 5,price=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:6", "payload=6 7 8 0,price=6"));

    // inc build, optimize merge
    string incDoc2 = "cmd=add,key=7,payload=7 8 9 10,price=7,ts=2;"
                     "cmd=update_field,key=4,payload=4 7 6 7,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc2, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "payload=0 3 2 1,price=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:4", "payload=4 7 6 7,price=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:7", "payload=7 8 9 10,price=7"));

    // rt build
    string rtDoc = "cmd=add,key=8,payload=8 9 10 11,price=8,ts=3;"
                   "cmd=update_field,key=6,payload=6 7 8 9,ts=4;"
                   "cmd=update_field,key=8,payload=8 11 10 9,ts=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:8", "payload=8 11 10 9,price=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:6", "payload=6 7 8 9,price=6"));
}

template <typename T>
void IndexBuilderInteTest::assertAttribute(const SegmentMetrics& metrics,
    const std::string& attrName, T attrMin, T attrMax, size_t checkedDocCount)
{
    auto groupMetrics = metrics.GetSegmentCustomizeGroupMetrics();
    ASSERT_TRUE(groupMetrics.get());
    size_t actualChecked = groupMetrics->Get<size_t>("checked_doc_count");
    ASSERT_EQ(checkedDocCount, actualChecked);
    if (actualChecked == 0)
    {
        return;
    }
    string attrKey = ATTRIBUTE_IDENTIFIER + ":" + attrName;
    auto attrValues = groupMetrics->Get<json::JsonMap>(attrKey);
    T actualMin = json::JsonNumberCast<T>(attrValues["min"]);
    T actualMax = json::JsonNumberCast<T>(attrValues["max"]);
    ASSERT_EQ(attrMin, actualMin);
    ASSERT_EQ(attrMax, actualMax);
}

index_base::SegmentMetrics IndexBuilderInteTest::LoadSegmentMetrics(
    test::PartitionStateMachine& psm, segmentid_t segId)
{
    auto partData = psm.GetIndexPartition()->GetPartitionData();
    auto segData = partData->GetSegmentData(segId);
    return segData.GetSegmentMetrics();
}

void IndexBuilderInteTest::assertLifecycle(const SegmentMetrics& metrics, const string& expected)
{
    auto groupMetrics = metrics.GetSegmentCustomizeGroupMetrics();
    ASSERT_TRUE(groupMetrics.get());
    auto actualLifecycle = groupMetrics->Get<string>(LIFECYCLE);
    EXPECT_EQ(expected, actualLifecycle);
}

void IndexBuilderInteTest::CheckFileStat(
        file_system::IndexlibFileSystemPtr fileSystem, string filePath,
        FSOpenType expectOpenType, FSFileType expectFileType)
{
    SCOPED_TRACE(filePath);
    string absPath = PathUtil::JoinPath(GET_TEST_DATA_PATH(), filePath);
    file_system::FileStat fileStat;
    fileSystem->GetFileStat(absPath, fileStat);
    ASSERT_EQ(expectOpenType, fileStat.openType);
    ASSERT_EQ(expectFileType, fileStat.fileType);
}


void IndexBuilderInteTest::TestForTimeSeriesStrategy()
{

    IndexPartitionOptions options;

    string field = "string1:string;string2:string;time:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "string1;time";

    string subField = "sub_time:uint32;sub_string1:string;";
    string subIndex = "sub_pk:primarykey64:sub_string1;";
    string subAttr = "sub_time;sub_string1";

    string docStrings = "cmd=add,string1=a,time=20,string2=test1,sub_string1=b^k,sub_time=10;"
                        "cmd=add,string1=d,time=40,string2=test2,sub_string1=e,sub_time=10;"
                        "cmd=add,string1=f,time=10,string2=test3,sub_string1=f,sub_time=100;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(subField, subIndex, subAttr, "");
    schema->SetSubIndexPartitionSchema(subSchema);

    options.GetMergeConfig().mergeStrategyStr = "time_series";
    options.GetMergeConfig().mergeStrategyParameter.inputLimitParam
        = "max-doc-count=30;sort-field=time;";
    options.GetMergeConfig().mergeStrategyParameter.strategyConditions
        = "time=[ - 50], output-interval=100;time = [50 - 100], output = 300;time = [100 - "
        "500],output= 500;time = [500-], output = 600";
    options.GetMergeConfig().mergeStrategyParameter.outputLimitParam
        = "max-merged-segment-doc-count=100";

    std::string splitConfigStr
        = "{\"class_name\":\"time_series\",\"parameters\":{\"attribute\":\"time\",\"ranges\":\"50, 100, 200, 500\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), splitConfigStr);


    BuildConfig& buildConfig = options.GetBuildConfig(false);
    auto& updaterConfigs = buildConfig.GetSegmentMetricsUpdaterConfig();
    updaterConfigs.resize(1);
    auto& updaterConfig = updaterConfigs[0];
    updaterConfig.className = LifecycleSegmentMetricsUpdater::UPDATER_NAME;
    updaterConfig.parameters["attribute_name"] = "time";
    updaterConfig.parameters["default_lifecycle_tag"] = "hot";
    updaterConfig.parameters["lifecycle_param"] = "0:hot;30:cold";

    string jsonStr = R"(
    {
        "load_config" :
        [
            {
                "file_patterns" : ["_INDEX_"],
                "load_strategy" : "mmap",
                "lifecycle": "hot",
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            {
                "file_patterns" : ["_INDEX_"],
                "load_strategy" : "cache",
                "lifecycle": "cold",
                "load_strategy_param" : {
                     "global_cache" : true
                }
            },
            {
                "file_patterns" : ["_ATTRIBUTE_"],
                "load_strategy" : "mmap",
                "lifecycle": "hot",
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            {
                "file_patterns" : ["_ATTRIBUTE_"],
                "load_strategy" : "mmap",
                "lifecycle": "cold",
                "load_strategy_param" : {
                     "lock" : false
                }
            }
        ]
    })";

    options.GetOnlineConfig().loadConfigList =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docStrings, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:a", "docid=0,time=20"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:k", "docid=1"));
    auto seg1Metrics = LoadSegmentMetrics(psm, 1);
    assertLifecycle(seg1Metrics, "hot");
    assertAttribute(seg1Metrics, "time", 10, 40, 3);

    string incDoc = "cmd=add,string1=incA,time=20,string2=test4,sub_string1=incB,sub_time=40;"
                    "cmd=add,string1=incD,time=40,string2=test5,sub_string1=incC,sub_time=50;"
                    "cmd=add,string1=incF,time=80,string2=test6,sub_string1=incE,sub_time=100;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:incA", "docid=5,time=20"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:incF", "docid=2,time=80"));

    auto onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    auto ifs = onlinePartition->GetFileSystem();
    auto seg3Metrics = LoadSegmentMetrics(psm, 3);
    assertLifecycle(seg3Metrics, "hot");
    assertAttribute(seg3Metrics, "time", 40, 80, 3);
    CheckFileStat(ifs, "segment_3_level_0/index/index2/posting", FSOT_LOAD_CONFIG, FSFT_MMAP_LOCK);
    CheckFileStat(ifs, "segment_3_level_0/attribute/time/data", FSOT_MMAP, FSFT_MMAP_LOCK);

    auto seg4Metrics = LoadSegmentMetrics(psm, 4);
    assertLifecycle(seg4Metrics, "cold");
    assertAttribute(seg4Metrics, "time", 10, 20, 3);
    CheckFileStat(ifs, "segment_4_level_0/index/index2/posting", FSOT_LOAD_CONFIG, FSFT_BLOCK);
    CheckFileStat(ifs, "segment_4_level_0/attribute/time/data", FSOT_MMAP, FSFT_MMAP);


}

IE_NAMESPACE_END(partition);
