#include "indexlib/partition/segment/test/in_memory_segment_creator_unittest.h"

#include "indexlib/config/test/schema_maker.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/segment/sub_doc_segment_writer.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::common;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemorySegmentCreatorTest);

InMemorySegmentCreatorTest::InMemorySegmentCreatorTest() { mCounterMap.reset(new CounterMap()); }

InMemorySegmentCreatorTest::~InMemorySegmentCreatorTest() {}

void InMemorySegmentCreatorTest::CaseSetUp()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void InMemorySegmentCreatorTest::CaseTearDown() {}

void InMemorySegmentCreatorTest::TestExtractNonTruncateIndexNames()
{
    {
        // no sub schema
        IndexPartitionSchemaPtr schema = CreateSchema(false);
        vector<string> indexNames;
        InMemorySegmentCreator::ExtractNonTruncateIndexNames(schema, indexNames);
        ASSERT_EQ((size_t)2, indexNames.size());
        ASSERT_EQ(string("pk"), indexNames[0]);
        ASSERT_EQ(string("pack1"), indexNames[1]);
    }

    {
        // has sub schema
        IndexPartitionSchemaPtr schema = CreateSchema(true);
        vector<string> indexNames;
        InMemorySegmentCreator::ExtractNonTruncateIndexNames(schema, indexNames);
        ASSERT_EQ((size_t)4, indexNames.size());
        ASSERT_EQ(string("pk"), indexNames[0]);
        ASSERT_EQ(string("pack1"), indexNames[1]);
        ASSERT_EQ(string("subindex1"), indexNames[2]);
        ASSERT_EQ(string("sub_pk"), indexNames[3]);
    }

    {
        // has truncate
        IndexPartitionSchemaPtr schema = CreateSchema(false);
        IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("pack1");
        indexConfig->SetNonTruncateIndexName("non_trunc_pack1");
        vector<string> indexNames;
        InMemorySegmentCreator::ExtractNonTruncateIndexNames(schema, indexNames);
        ASSERT_EQ((size_t)1, indexNames.size());
        ASSERT_EQ(string("pk"), indexNames[0]);
    }
}

void InMemorySegmentCreatorTest::TestCreateSubWriterOOM()
{
    IndexPartitionSchemaPtr schema = CreateSchema(true);
    IndexPartitionOptions options;
    InMemorySegmentCreator creator;
    creator.Init(schema, options);
    creator.mSegmentMetrics->SetDistinctTermCount("subindex1", 5 * 1024 * 1024);
    BuildingSegmentData segData;
    segData.SetSubSegmentData(SegmentDataPtr(new BuildingSegmentData));
    CounterMapPtr counterMap;
    plugin::PluginManagerPtr nullPluginManager;
    InMemorySegmentPtr inMemSegment = creator.Create(
        PartitionSegmentIteratorPtr(), segData, GET_SEGMENT_DIRECTORY(),
        util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE),
        counterMap, nullPluginManager);

    ASSERT_TRUE(inMemSegment);
    SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
    string docString = "cmd=add,pkstr=0,text1=2,subpkstr=1,substr1=2;";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
    segWriter->AddDocument(doc);
    size_t expectSize = autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE;
    ASSERT_TRUE(segWriter->GetTotalMemoryUse() < expectSize);
}

void InMemorySegmentCreatorTest::TestCreateSegmentWriterOutOfMem()
{
    IndexPartitionSchemaPtr schema = CreateSchema(false);
    IndexPartitionOptions options;
    InMemorySegmentCreator creator;
    creator.Init(schema, options);
    creator.mSegmentMetrics->SetDistinctTermCount("pack1", 5 * 1024 * 1024);

    BuildingSegmentData segData;
    CounterMapPtr counterMap;
    plugin::PluginManagerPtr nullPluginManager;
    InMemorySegmentPtr inMemSegment = creator.Create(
        PartitionSegmentIteratorPtr(), segData, GET_SEGMENT_DIRECTORY(),
        util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE),
        counterMap, nullPluginManager);
    ASSERT_TRUE(inMemSegment);
    ASSERT_FALSE(inMemSegment->GetSubInMemorySegment());
    SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();

    string docString = "cmd=add,pkstr=0,text1=2;";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docString);
    segWriter->AddDocument(doc);
    size_t expectSize = autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE;
    cout << segWriter->GetTotalMemoryUse() << endl;
    ASSERT_TRUE(segWriter->GetTotalMemoryUse() < expectSize);
}

void InMemorySegmentCreatorTest::TestInit()
{
    {
        // not set init hash map size
        IndexPartitionSchemaPtr schema = CreateSchema(false);
        vector<string> indexNames;
        InMemorySegmentCreator::ExtractNonTruncateIndexNames(schema, indexNames);

        IndexPartitionOptions options;
        options.SetIsOnline(false);
        BuildConfig& buildConfig = options.GetBuildConfig();
        InMemorySegmentCreator creator;
        creator.Init(schema, options);
        CheckSegmentMetrics(creator.mSegmentMetrics, indexNames,
                            buildConfig.buildTotalMemory / indexNames.size() / 2 * 800);
    }

    {
        // set init hash map size
        IndexPartitionSchemaPtr schema = CreateSchema(false);
        vector<string> indexNames;
        InMemorySegmentCreator::ExtractNonTruncateIndexNames(schema, indexNames);

        IndexPartitionOptions options;
        options.SetIsOnline(false);
        BuildConfig& buildConfig = options.GetBuildConfig();
        buildConfig.hashMapInitSize = 10000;
        InMemorySegmentCreator creator;
        creator.Init(schema, options);
        CheckSegmentMetrics(creator.mSegmentMetrics, indexNames, 10000);
    }
}

void InMemorySegmentCreatorTest::TestCreateWithoutSubDoc()
{
    IndexPartitionSchemaPtr schema = CreateSchema(false);
    IndexPartitionOptions options;
    InMemorySegmentCreator creator;
    creator.Init(schema, options);

    BuildingSegmentData segData;
    plugin::PluginManagerPtr nullPluginManager;
    InMemorySegmentPtr inMemSegment = creator.Create(
        PartitionSegmentIteratorPtr(), segData, GET_SEGMENT_DIRECTORY(),
        util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(), mCounterMap, nullPluginManager);
    ASSERT_TRUE(inMemSegment);
    ASSERT_FALSE(inMemSegment->GetSubInMemorySegment());

    CheckSegmentWriter(inMemSegment->GetSegmentWriter(), false);
}

void InMemorySegmentCreatorTest::TestCreateWithSubDoc()
{
    IndexPartitionSchemaPtr schema = CreateSchema(true);
    IndexPartitionOptions options;
    InMemorySegmentCreator creator;
    creator.Init(schema, options);

    BuildingSegmentData segData;
    segData.SetSubSegmentData(SegmentDataPtr(new BuildingSegmentData));

    plugin::PluginManagerPtr nullPluginManager;
    InMemorySegmentPtr inMemSegment = creator.Create(
        PartitionSegmentIteratorPtr(), segData, GET_SEGMENT_DIRECTORY(),
        util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(), mCounterMap, nullPluginManager);
    ASSERT_TRUE(inMemSegment);
    InMemorySegmentPtr subInMemSegment = inMemSegment->GetSubInMemorySegment();
    ASSERT_TRUE(subInMemSegment);

    CheckSegmentWriter(inMemSegment->GetSegmentWriter(), true);
    CheckSegmentWriter(subInMemSegment->GetSegmentWriter(), false);
}

void InMemorySegmentCreatorTest::TestCreateWithOperationWriter()
{
    // test online
    {
        IndexPartitionSchemaPtr schema = CreateSchema(false);
        IndexPartitionOptions options;
        options.SetIsOnline(true);
        InMemorySegmentCreator creator;
        creator.Init(schema, options);
        BuildingSegmentData segData;
        plugin::PluginManagerPtr nullPluginManager;
        InMemorySegmentPtr inMemSegment = creator.Create(
            PartitionSegmentIteratorPtr(), segData, GET_SEGMENT_DIRECTORY(),
            util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(), mCounterMap, nullPluginManager);

        OperationWriterPtr opWriter = DYNAMIC_POINTER_CAST(OperationWriter, inMemSegment->GetOperationWriter());
        ASSERT_TRUE(opWriter);
    }
    // test offline
    {
        IndexPartitionSchemaPtr schema = CreateSchema(false);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        InMemorySegmentCreator creator;
        creator.Init(schema, options);
        BuildingSegmentData segData;
        plugin::PluginManagerPtr nullPluginManager;
        InMemorySegmentPtr inMemSegment = creator.Create(
            PartitionSegmentIteratorPtr(), segData, GET_SEGMENT_DIRECTORY(),
            util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(), mCounterMap, nullPluginManager);

        OperationWriterPtr opWriter = DYNAMIC_POINTER_CAST(OperationWriter, inMemSegment->GetOperationWriter());
        ASSERT_FALSE(opWriter);
    }
}

IndexPartitionSchemaPtr InMemorySegmentCreatorTest::CreateSchema(bool hasSub)
{
    string field = "pkstr:string;text1:text;long1:uint32;multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pk:primarykey64:pkstr;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "pkstr;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    if (hasSub) {
        IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "substr1:string;subpkstr:string;sub_long:uint32;", "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
            "substr1;subpkstr;sub_long;", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        SchemaRewriter::RewriteForSubTable(schema);
    }
    return schema;
}

void InMemorySegmentCreatorTest::CheckSegmentMetrics(
    const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics, const vector<string>& indexNames,
    size_t expectTermCount)
{
    ASSERT_TRUE(segmentMetrics);
    for (size_t i = 0; i < indexNames.size(); i++) {
        ASSERT_EQ(expectTermCount, segmentMetrics->GetDistinctTermCount(indexNames[i]));
    }
}

void InMemorySegmentCreatorTest::CheckSegmentWriter(SegmentWriterPtr segWriter, bool isSubDocWriter)
{
    if (isSubDocWriter) {
        SubDocSegmentWriterPtr subDocSegWriter = DYNAMIC_POINTER_CAST(SubDocSegmentWriter, segWriter);
        ASSERT_TRUE(subDocSegWriter);
        return;
    }

    SingleSegmentWriterPtr singleSegWriter = DYNAMIC_POINTER_CAST(SingleSegmentWriter, segWriter);
    ASSERT_TRUE(singleSegWriter);
}
}} // namespace indexlib::partition
