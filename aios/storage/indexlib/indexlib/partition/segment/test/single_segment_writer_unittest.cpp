#include "indexlib/partition/segment/test/single_segment_writer_unittest.h"

#include "indexlib/common/dump_item.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/module_info.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/modifier/in_memory_segment_modifier.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace fslib;
using namespace autil;

using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::test;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SingleSegmentWriterTest);

SingleSegmentWriterTest::SingleSegmentWriterTest() {}

SingleSegmentWriterTest::~SingleSegmentWriterTest() {}

void SingleSegmentWriterTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema,
                                     "string1:string;", // above is field schema
                                     "stringI:string:string1;"
                                     "pk:primarykey64:string1", // above is index schema
                                     "string1",                 // above is attribute schema
                                     "string1");                // above is summary schema

    mDocStr = "{ 1, " // doc payload
              "[string1: (token1)],"
              "};"
              "{ 2, " // doc payload
              "[string1: (token2)],"
              "};";

    mSegmentMetrics.reset(new indexlib::framework::SegmentMetrics);
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void SingleSegmentWriterTest::CaseTearDown() {}

void SingleSegmentWriterTest::TestEstimateInitMemUse()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema,
                                     "multi_long:int64:true:true:uniq;"
                                     "string1:string;",
                                     "pk:primarykey64:string1;"
                                     "index1:string:string1",
                                     "multi_long", "multi_long");

    mOptions.GetBuildConfig().dumpThreadCount = 1;
    string docString = "cmd=add,string1=hello0,multi_long=2 3;";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
    mSegmentMetrics->SetDistinctTermCount("index1", 5 * 1024 * 1024);
    SingleSegmentWriter segWriter(schema, mOptions);
    size_t estimateMemUse = SingleSegmentWriter::EstimateInitMemUse(
        mSegmentMetrics, schema, mOptions, plugin::PluginManagerPtr(), PartitionSegmentIteratorPtr());
    segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                   plugin::PluginManagerPtr());
    cout << "after init:" << segWriter.GetTotalMemoryUse() << endl;
    segWriter.AddDocument(doc);
    cout << "estimate mem use" << estimateMemUse << endl;
    size_t extraSize = autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE;
    ASSERT_TRUE(segWriter.GetTotalMemoryUse() < estimateMemUse + extraSize);
    ASSERT_TRUE(segWriter.GetTotalMemoryUse() > estimateMemUse);
}

void SingleSegmentWriterTest::TestEstimateInitMemUseWithShardingIndex()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("multi_long:int64:true:true:uniq;"
                                                             "string1:string;",
                                                             "pk:primarykey64:string1;"
                                                             "index1:string:string1:false:2",
                                                             "multi_long", "multi_long");

    mOptions.GetBuildConfig().dumpThreadCount = 1;
    // mSegmentMetrics->SetDistinctTermCount("index1", 5 * 1024 * 1024);
    mSegmentMetrics->SetDistinctTermCount("index1_@_0", 5 * 512 * 1024);
    mSegmentMetrics->SetDistinctTermCount("index1_@_1", 5 * 512 * 1024);
    SingleSegmentWriter segWriter(schema, mOptions);
    size_t estimateMemUse = SingleSegmentWriter::EstimateInitMemUse(
        mSegmentMetrics, schema, mOptions, plugin::PluginManagerPtr(), PartitionSegmentIteratorPtr());
    segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                   plugin::PluginManagerPtr());
    cout << "after init:" << segWriter.GetTotalMemoryUse() << endl;

    // add 2 docs for sharding indexes
    string docString = "cmd=add,string1=0,multi_long=2 3;";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
    segWriter.AddDocument(doc);

    docString = "cmd=add,string1=2,multi_long=2 3;";
    doc = DocumentCreator::CreateNormalDocument(schema, docString);
    segWriter.AddDocument(doc);

    cout << "estimate mem use" << estimateMemUse << endl;
    cout << "actual mem use" << segWriter.GetTotalMemoryUse() << endl;
    size_t extraSize = autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE;
    ASSERT_TRUE(segWriter.GetTotalMemoryUse() < estimateMemUse + extraSize);
    ASSERT_TRUE(segWriter.GetTotalMemoryUse() > estimateMemUse);
}

void SingleSegmentWriterTest::TestCloneInPrivateMode()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("multi_long:int64:true:true:uniq;"
                                                             "string1:string;",
                                                             "pk:primarykey64:string1;"
                                                             "index1:string:string1:false:2",
                                                             "multi_long", "multi_long");

    mOptions.GetBuildConfig().dumpThreadCount = 1;
    mSegmentMetrics->SetDistinctTermCount("index1_@_0", 5 * 512 * 1024);
    mSegmentMetrics->SetDistinctTermCount("index1_@_1", 5 * 512 * 1024);
    SingleSegmentWriter segWriter(schema, mOptions);
    segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                   plugin::PluginManagerPtr());

    // add 2 docs for sharding indexes
    string docString = "cmd=add,string1=0,multi_long=2 3;";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
    segWriter.AddDocument(doc);

    docString = "cmd=add,string1=2,multi_long=2 3;";
    doc = DocumentCreator::CreateNormalDocument(schema, docString);
    segWriter.AddDocument(doc);
    BuildingSegmentData buildingSegData;
    buildingSegData.SetSegmentId(0);
    SingleSegmentWriter* clonedWriter = segWriter.CloneWithNewSegmentData(buildingSegData, false);
    auto newDeletionMapReader = clonedWriter->CreateInMemSegmentReader()->GetInMemDeletionMapReader();
    auto oldDeletionMapReader = segWriter.CreateInMemSegmentReader()->GetInMemDeletionMapReader();
    ASSERT_FALSE(oldDeletionMapReader->IsDeleted(0));
    ASSERT_FALSE(newDeletionMapReader->IsDeleted(0));
    newDeletionMapReader->Delete(0);
    ASSERT_FALSE(oldDeletionMapReader->IsDeleted(0));
    ASSERT_TRUE(newDeletionMapReader->IsDeleted(0));
    delete clonedWriter;
}

void SingleSegmentWriterTest::TestSimpleProcess()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);

    SingleSegmentWriter segWriter(mSchema, mOptions);
    segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                   plugin::PluginManagerPtr());

    DocumentMaker::DocumentVect docVect;
    DocumentMaker::MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(mDocStr, mSchema, docVect, mockIndexPart);

    for (size_t i = 0; i < docVect.size(); i++) {
        segWriter.AddDocument(docVect[i]);
    }

    SegmentInfoPtr segmentInfo = segWriter.GetSegmentInfo();
    ASSERT_EQ((size_t)2, segmentInfo->docCount);

    InMemorySegmentReaderPtr reader = segWriter.CreateInMemSegmentReader();
    ASSERT_TRUE(reader);
    AttributeSegmentReaderPtr attrReader = reader->GetAttributeSegmentReader("string1");
    ASSERT_TRUE(attrReader);
    uint32_t dataLength = attrReader->TEST_GetDataLength(0);
    ASSERT_EQ((uint32_t)7, dataLength);
    // TODO: check attribute value, check index

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), mOptions, mSchema);
    file_system::DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(0);
    segWriter.EndSegment();
    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    segWriter.CreateDumpItems(directory, dumpItems);
    // two index work items, one attribute work item
    // and one deletionmap work item,
    // no summary item(all summary fields are also in attribute schems)
    ASSERT_EQ((size_t)4, dumpItems.size());
    for (size_t i = 0; i < dumpItems.size(); ++i) {
        auto dumpItem = dumpItems[i].release();
        dumpItem->destroy();
    }
}

void SingleSegmentWriterTest::TestSimpleProcessForCustomizedIndex()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);

    IndexPartitionSchemaPtr schema =
        SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "customized_index_schema.json");

    SingleSegmentWriter segWriter(schema, mOptions);
    segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                   plugin::PluginManagerPtr());

    string docStr = "{ 1, " // doc payload
                    "[cfield1: hello],"
                    "[pk: (token1)],"
                    "};"
                    "{ 2, " // doc payload
                    "[cfield1: hello world],"
                    "[pk: (token2)],"
                    "};";

    DocumentMaker::DocumentVect docVect;
    DocumentMaker::MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(docStr, schema, docVect, mockIndexPart);

    for (size_t i = 0; i < docVect.size(); i++) {
        segWriter.AddDocument(docVect[i]);
    }

    SegmentInfoPtr segmentInfo = segWriter.GetSegmentInfo();
    ASSERT_EQ((size_t)2, segmentInfo->docCount);

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), mOptions, schema);
    file_system::DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(0);
    segWriter.EndSegment();
    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    segWriter.CreateDumpItems(directory, dumpItems);
    // two index work items: pk & test_customize
    // and one deletionmap work item
    ASSERT_EQ((size_t)3, dumpItems.size());
    for (size_t i = 0; i < dumpItems.size(); ++i) {
        auto dumpItem = dumpItems[i].release();
        dumpItem->process(&mPool);
        dumpItem->destroy();
    }
    DirectoryPtr indexDirectory = directory->GetDirectory(INDEX_DIR_NAME, true);
    ASSERT_TRUE(indexDirectory->IsExist("test_customize/demo_index_file"));
}

void SingleSegmentWriterTest::TestGetTotalMemoryUse()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);

    SingleSegmentWriter segWriter(mSchema, mOptions);
    segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                   plugin::PluginManagerPtr());

    size_t beforeTotalMemoryUse = segWriter.GetTotalMemoryUse();
    util::BuildResourceMetricsPtr buildResourceMetrics = segWriter.mBuildResourceMetrics;
    util::BuildResourceMetricsNode* metricsNode1 = buildResourceMetrics->AllocateNode();
    util::BuildResourceMetricsNode* metricsNode2 = buildResourceMetrics->AllocateNode();
    util::BuildResourceMetricsNode* metricsNode3 = buildResourceMetrics->AllocateNode();

    metricsNode1->Update(util::BMT_CURRENT_MEMORY_USE, 16);
    metricsNode2->Update(util::BMT_CURRENT_MEMORY_USE, 28);
    metricsNode3->Update(util::BMT_CURRENT_MEMORY_USE, 42);

    size_t totalMemUse = 16 + 28 + 42;
    ASSERT_EQ(totalMemUse, segWriter.GetTotalMemoryUse() - beforeTotalMemoryUse);
}

void SingleSegmentWriterTest::TestCaseForInitSegmentMetrics()
{
    SingleSegmentWriter writer(mSchema, mOptions);
    SegmentData segmentData;
    segmentData.SetBaseDocId(0);
    segmentData.SetSegmentId(0);

    mSegmentMetrics->SetDistinctTermCount("stringI", 100);
    writer.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                plugin::PluginManagerPtr());

    // make dirty
    SegmentInfoPtr segInfo = writer.GetSegmentInfo();
    segInfo->docCount = 1;

    writer.CollectSegmentMetrics();
    ASSERT_EQ((size_t)0, mSegmentMetrics->GetDistinctTermCount("stringI"));
}

void SingleSegmentWriterTest::TestInit()
{
    IndexPartitionSchemaPtr schema = CreateSchemaWithVirtualAttribute();
    SingleSegmentWriter writer(schema, mOptions);
    SegmentData segmentData;
    segmentData.SetBaseDocId(0);
    segmentData.SetSegmentId(0);

    writer.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                plugin::PluginManagerPtr());

    ASSERT_TRUE(writer.mCurrentSegmentInfo);
    ASSERT_TRUE(writer._inMemoryIndexSegmentWriter);
    ASSERT_TRUE(writer._inMemoryAttributeSegmentWriter);
    ASSERT_TRUE(writer._virtualInMemoryAttributeSegmentWriter);
    ASSERT_TRUE(writer._summaryWriter);
    ASSERT_TRUE(writer.mDeletionMapSegmentWriter);
    ASSERT_TRUE(writer.mModifier);
}

void SingleSegmentWriterTest::TestCreateInMemSegmentReader()
{
    IndexPartitionSchemaPtr schema = CreateSchemaWithVirtualAttribute();
    SingleSegmentWriter writer(schema, mOptions);
    SegmentData segmentData;
    segmentData.SetBaseDocId(0);
    segmentData.SetSegmentId(5);

    writer.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                plugin::PluginManagerPtr());
    SegmentInfoPtr segInfo = writer.GetSegmentInfo();
    segInfo->docCount = 1;
    segInfo->timestamp = 100;

    InMemorySegmentReaderPtr inMemSegReader = writer.CreateInMemSegmentReader();
    ASSERT_TRUE(inMemSegReader);

    SegmentInfo inMemSegInfo = inMemSegReader->GetSegmentInfo();
    ASSERT_EQ(*segInfo, inMemSegInfo);
    ASSERT_EQ((segmentid_t)5, inMemSegReader->GetSegmentId());

    CheckInMemorySegmentReader(schema, inMemSegReader);

    // check deletion map related with docCount
    InMemDeletionMapReaderPtr inMemDeletionMapReader = inMemSegReader->GetInMemDeletionMapReader();
    ASSERT_FALSE(inMemDeletionMapReader->IsDeleted(0));
    ASSERT_TRUE(inMemDeletionMapReader->IsDeleted(1));

    segInfo->docCount = segInfo->docCount + 1;
    ASSERT_FALSE(inMemDeletionMapReader->IsDeleted(0));
    ASSERT_FALSE(inMemDeletionMapReader->IsDeleted(1));
    ASSERT_TRUE(inMemDeletionMapReader->IsDeleted(2));
}

void SingleSegmentWriterTest::TestIsDirty()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);

    SingleSegmentWriter segWriter(mSchema, mOptions);
    segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                   plugin::PluginManagerPtr());

    SegmentInfoPtr segInfo = segWriter.GetSegmentInfo();
    InMemorySegmentModifierPtr segModifer = segWriter.GetInMemorySegmentModifier();

    ASSERT_FALSE(segWriter.IsDirty());
    segInfo->docCount = 1;
    ASSERT_TRUE(segWriter.IsDirty());

    segInfo->docCount = 0;
    segModifer->RemoveDocument(0);
    ASSERT_TRUE(segWriter.IsDirty());
}

void SingleSegmentWriterTest::TestDump()
{
    IndexPartitionSchemaPtr schema = CreateSchemaWithVirtualAttribute(true);

    SegmentData segmentData;
    segmentData.SetSegmentId(0);
    SingleSegmentWriter segWriter(schema, mOptions);
    segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                   plugin::PluginManagerPtr());

    // make one add doc
    string docString = "cmd=add,string1=hello0,price=200";
    autil::mem_pool::Pool* pool = new autil::mem_pool::Pool;
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString, true, pool);
    segWriter.AddDocument(doc);

    InMemorySegmentModifierPtr segModifer = segWriter.GetInMemorySegmentModifier();
    segModifer->RemoveDocument(0);

    DirectoryPtr segDirectory = GET_SEGMENT_DIRECTORY();
    segWriter.EndSegment();
    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    segWriter.CreateDumpItems(segDirectory, dumpItems);
    for (size_t i = 0; i < dumpItems.size(); ++i) {
        DumpItem* workItem = dumpItems[i].release();
        workItem->process(&mPool);
        workItem->destroy();
    }
    ASSERT_TRUE(segDirectory->IsExist(DELETION_MAP_DIR_NAME));
    ASSERT_TRUE(segDirectory->IsExist(INDEX_DIR_NAME));
    ASSERT_TRUE(segDirectory->IsExist(ATTRIBUTE_DIR_NAME));
    ASSERT_TRUE(segDirectory->IsExist(SUMMARY_DIR_NAME));
    ASSERT_TRUE(segDirectory->IsExist(SOURCE_DIR_NAME));

    DirectoryPtr deletionMapDirectory = segDirectory->GetDirectory(DELETION_MAP_DIR_NAME, true);
    DirectoryPtr indexDirectory = segDirectory->GetDirectory(INDEX_DIR_NAME, true);
    DirectoryPtr attrDirectory = segDirectory->GetDirectory(ATTRIBUTE_DIR_NAME, true);
    DirectoryPtr summaryDirectory = segDirectory->GetDirectory(SUMMARY_DIR_NAME, true);
    DirectoryPtr sourceDirectory = segDirectory->GetDirectory(SOURCE_DIR_NAME, true);

    ASSERT_TRUE(deletionMapDirectory->IsExist("data_0"));
    ASSERT_TRUE(indexDirectory->IsExist("pk/data"));
    ASSERT_TRUE(indexDirectory->IsExist("pk/attribute_string1/data"));

    ASSERT_TRUE(attrDirectory->IsExist("biz30day/data"));
    ASSERT_TRUE(attrDirectory->IsExist("price/data"));
    ASSERT_TRUE(attrDirectory->IsExist("vir_attr/data"));
    ASSERT_TRUE(summaryDirectory->IsExist("offset"));
    ASSERT_TRUE(summaryDirectory->IsExist("data"));

    ASSERT_TRUE(sourceDirectory->IsExist("meta/data"));
    ASSERT_TRUE(sourceDirectory->IsExist("meta/offset"));
    ASSERT_TRUE(sourceDirectory->IsExist("group_0/data"));
    ASSERT_TRUE(sourceDirectory->IsExist("group_0/offset"));
    delete pool;
}

void SingleSegmentWriterTest::TestDumpEmptySegment()
{
    IndexPartitionSchemaPtr schema = CreateSchemaWithVirtualAttribute();

    SegmentData segmentData;
    segmentData.SetSegmentId(0);
    SingleSegmentWriter segWriter(schema, mOptions);
    segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                   plugin::PluginManagerPtr());

    // dump empty segment
    DirectoryPtr segDirectory = GET_SEGMENT_DIRECTORY();
    segWriter.EndSegment();
    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    segWriter.CreateDumpItems(segDirectory, dumpItems);
    ASSERT_TRUE(dumpItems.empty());
}

void SingleSegmentWriterTest::TestDumpSegmentWithCopyOnDumpAttribute()
{
    mOptions.SetIsOnline(false);
    InnerTestDumpSegmentWithCopyOnDumpAttribute(mOptions);

    mOptions.SetIsOnline(true);
    OnlineConfig& onlineConfig = mOptions.GetOnlineConfig();
    onlineConfig.onDiskFlushRealtimeIndex = true;
    InnerTestDumpSegmentWithCopyOnDumpAttribute(mOptions);

    onlineConfig.onDiskFlushRealtimeIndex = false;
    InnerTestDumpSegmentWithCopyOnDumpAttribute(mOptions);
}

IndexPartitionSchemaPtr SingleSegmentWriterTest::CreateSchemaWithVirtualAttribute(bool enableSource)
{
    string field = "string1:string;price:uint32;biz30day:float";
    string index = "pk:primarykey64:string1";
    string attribute = "biz30day;price";
    string summary = "string1";
    string source;
    if (enableSource) {
        source = "string1#price#biz30day";
    }
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, summary, "", source);

    // set default value for attribute
    FieldConfigPtr fieldConfig = schema->GetFieldConfig("biz30day");

    // add virtual attribute
    FieldConfigPtr virtualFieldConfig(new FieldConfig("vir_attr", ft_int64, false));

    // set default value
    virtualFieldConfig->SetDefaultValue("100");

    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_virtual));
    attrConfig->Init(virtualFieldConfig);
    schema->AddVirtualAttributeConfig(attrConfig);
    return schema;
}

void SingleSegmentWriterTest::CheckInMemorySegmentReader(const IndexPartitionSchemaPtr& schema,
                                                         const InMemorySegmentReaderPtr inMemSegReader)
{
    ASSERT_TRUE(inMemSegReader->GetInMemDeletionMapReader());

    if (schema->NeedStoreSummary()) {
        ASSERT_TRUE(inMemSegReader->GetSummaryReader());
    }

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    for (size_t i = 0; i < indexSchema->GetIndexCount(); i++) {
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig((indexid_t)i);
        ASSERT_TRUE(
            inMemSegReader->GetMultiFieldIndexSegmentReader()->GetIndexSegmentReader(indexConfig->GetIndexName()));
    }

    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    if (attrSchema) {
        AttributeSchema::Iterator iter = attrSchema->Begin();
        for (; iter != attrSchema->End(); iter++) {
            ASSERT_TRUE(inMemSegReader->GetAttributeSegmentReader((*iter)->GetAttrName()));
        }
    }

    const AttributeSchemaPtr& virtualAttrSchema = schema->GetAttributeSchema();
    if (virtualAttrSchema) {
        AttributeSchema::Iterator iter = virtualAttrSchema->Begin();
        for (; iter != virtualAttrSchema->End(); iter++) {
            ASSERT_TRUE(inMemSegReader->GetAttributeSegmentReader((*iter)->GetAttrName()));
        }
    }
}

void SingleSegmentWriterTest::TestEstimateAttrDumpTemporaryMemoryUse()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema,
                                     "multi_long:int64:true:true:uniq;" // factor 32 + 4
                                     "string1:string;",                 // factor 4
                                     "pk:primarykey64:string1;"         // factor 12
                                     "index1:string:string1",           // min 32K * 8
                                     "string1;multi_long;", "");

    {
        mOptions.GetBuildConfig().dumpThreadCount = 1;
        SingleSegmentWriter segWriter(schema, mOptions);
        segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                       plugin::PluginManagerPtr());

        // make one add doc
        string docString = "cmd=add,string1=hello0,multi_long=2 3;";
        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
        segWriter.AddDocument(doc);

        const util::BuildResourceMetricsPtr& buildMetrics = segWriter.GetBuildResourceMetrics();
        EXPECT_EQ((int64_t)32 * 1024 * 8, util::BuildResourceCalculator::EstimateDumpTempMemoryUse(buildMetrics, 1));
    }
    {
        mOptions.GetBuildConfig().dumpThreadCount = 3;
        SingleSegmentWriter segWriter(schema, mOptions);
        segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                       plugin::PluginManagerPtr());

        // make one add doc
        string docString = "cmd=add,string1=hello0,multi_long=2 3;";
        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
        segWriter.AddDocument(doc);

        const util::BuildResourceMetricsPtr& buildMetrics = segWriter.GetBuildResourceMetrics();
        EXPECT_EQ((int64_t)32 * 1024 * 8 * 3,
                  util::BuildResourceCalculator::EstimateDumpTempMemoryUse(buildMetrics, 3));
    }
}

void SingleSegmentWriterTest::InnerTestDumpSegmentWithCopyOnDumpAttribute(const IndexPartitionOptions& options)
{
    tearDown();
    setUp();

    bool needFlush = options.IsOnline() ? options.GetOnlineConfig().onDiskFlushRealtimeIndex : true;

    file_system::FileSystemOptions fsOptions;
    fsOptions.enableAsyncFlush = true;
    fsOptions.useCache = true;
    fsOptions.useRootLink = false;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = FSST_MEM;
    RESET_FILE_SYSTEM("ut", true, fsOptions);

    IndexPartitionSchemaPtr schema = CreateSchemaWithVirtualAttribute();

    SegmentData segmentData;
    segmentData.SetSegmentId(0);
    SingleSegmentWriter segWriter(schema, options);
    segWriter.Init(segmentData, mSegmentMetrics, util::BuildResourceMetricsPtr(), util::CounterMapPtr(),
                   plugin::PluginManagerPtr());

    // make one add doc
    string docString = "cmd=add,string1=hello0,price=200";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
    segWriter.AddDocument(doc);

    DirectoryPtr segDirectory = GET_SEGMENT_DIRECTORY();
    segWriter.EndSegment();
    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    segWriter.CreateDumpItems(segDirectory, dumpItems);
    for (size_t i = 0; i < dumpItems.size(); ++i) {
        DumpItem* workItem = dumpItems[i].release();
        workItem->process(&mPool);
        workItem->destroy();
    }

    int64_t expectFlushMemUse = 0;
    if (options.IsOnline() && needFlush) {
        expectFlushMemUse = 16; // one doc : uint32 + uint32 + uint64(virtual)
    }

    StorageMetrics metrics = GET_FILE_SYSTEM()->GetFileSystemMetrics().GetOutputStorageMetrics();
    ASSERT_EQ(expectFlushMemUse, metrics.GetFlushMemoryUse());
}
}} // namespace indexlib::partition
