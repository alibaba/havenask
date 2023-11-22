#include "indexlib/partition/test/offline_partition_writer_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/SliceFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::test;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OfflinePartitionWriterTest);

namespace {
class MockSingleSegmentWriter : public SingleSegmentWriter
{
public:
    MockSingleSegmentWriter(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options)
        : SingleSegmentWriter(schema, options)
    {
    }
    MOCK_METHOD(bool, AddDocument, (const DocumentPtr& doc), (override));
    MOCK_METHOD(InMemorySegmentModifierPtr, GetInMemorySegmentModifier, (), (override));
    MOCK_METHOD(index_base::InMemorySegmentReaderPtr, CreateInMemSegmentReader, (), (override));
    MOCK_METHOD(void, EndSegment, (), (override));
    MOCK_METHOD(size_t, GetTotalMemoryUse, (), (const, override));
    MOCK_METHOD(void, Dump, (const DirectoryPtr& directory), ());
    MOCK_METHOD(bool, IsDirty, (), (const, override));
    MOCK_METHOD(size_t, EstimateMaxMemoryUseOfCurrentSegment, (), (const));
};

class MockOfflinePartitionWriter : public OfflinePartitionWriter
{
public:
    MockOfflinePartitionWriter(const config::IndexPartitionOptions& options)
        : OfflinePartitionWriter(options, DumpSegmentContainerPtr(new DumpSegmentContainer))
    {
    }
    MOCK_METHOD(size_t, EstimateMaxMemoryUseOfCurrentSegment, (), (const, override));
    MOCK_METHOD(size_t, GetResourceMemoryUse, (), (const, override));
    MOCK_METHOD(void, CleanResource, (), (const, override));
};

class MockPartitionModifier : public PatchModifier
{
public:
    MockPartitionModifier() : PatchModifier(IndexPartitionSchemaPtr()) {}
    ~MockPartitionModifier() {}

    MOCK_METHOD(size_t, EstimateMaxMemoryUseOfCurrentSegment, (), (const));
    MOCK_METHOD(bool, IsDirty, (), (const, override));
};
} // namespace

OfflinePartitionWriterTest::OfflinePartitionWriterTest() {}

OfflinePartitionWriterTest::~OfflinePartitionWriterTest() {}

void OfflinePartitionWriterTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    string field = "string1:string;";
    string index = "index1:string:string1;";
    mSchema = SchemaMaker::MakeSchema(field, index, "", "");
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void OfflinePartitionWriterTest::CaseTearDown() {}

void OfflinePartitionWriterTest::TestSimpleProcess()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);
    partitionData->CreateNewSegment();
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, mSchema, partitionData);

    string docString = "cmd=add,string1=hello";
    NormalDocumentPtr document = DocumentCreator::CreateNormalDocument(mSchema, docString);

    ASSERT_TRUE(writer.BuildDocument(document));
    writer.DumpSegment();
    ReOpenNewSegment(writer, mSchema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_TRUE(writer.BuildDocument(document));
    writer.Close();

    DirectoryPtr partitionDirectory = GET_PARTITION_DIRECTORY();
    Version version;
    ASSERT_TRUE(version.Load(partitionDirectory, "version.0"));
    ASSERT_EQ((size_t)2, version.GetSegmentCount());
    ASSERT_EQ((segmentid_t)0, version[0]);
    ASSERT_EQ((segmentid_t)1, version[1]);
}

void OfflinePartitionWriterTest::TestOpen()
{
    {
        // online mode open
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        InnerTestOpen(mSchema, options);
    }

    {
        // offline mode open
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.SetIsOnline(false);
        InnerTestOpen(mSchema, options);
    }

    {
        // schema with pk, online
        string field = "string1:string;";
        string index = "index1:string:string1;"
                       "pk:primarykey64:string1";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        InnerTestOpen(mSchema, options);
    }

    {
        // schema with pk, offline
        string field = "string1:string;";
        string index = "index1:string:string1;"
                       "pk:primarykey64:string1";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.SetIsOnline(false);
        InnerTestOpen(mSchema, options);
    }
}

void OfflinePartitionWriterTest::TestNeedDump()
{
    {
        // test no segment writer
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        DumpSegmentContainerPtr container(new DumpSegmentContainer);
        OfflinePartitionWriter writer(options, container);
        ASSERT_FALSE(writer.NeedDump(1024));
    }

    {
        // test max doc count trigger dump
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.SetIsOnline(false);
        options.GetBuildConfig().maxDocCount = 5;

        PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);
        MockOfflinePartitionWriter writer(options);
        OpenWriter(writer, mSchema, partitionData);

        writer.mSegmentInfo->docCount = 5;
        ASSERT_TRUE(writer.NeedDump(1024));
    }

    {
        // test for online
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.SetIsOnline(true);
        PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);

        MockOfflinePartitionWriter writer(options);
        OpenWriter(writer, mSchema, partitionData);
        MockSingleSegmentWriter* mockSegmentWriter = new MockSingleSegmentWriter(mSchema, options);
        SegmentWriterPtr segmentWriter(mockSegmentWriter);
        writer.mSegmentWriter = segmentWriter;

        EXPECT_CALL(writer, EstimateMaxMemoryUseOfCurrentSegment()).WillRepeatedly(Return(100));
        ASSERT_FALSE(writer.NeedDump(200));
        ASSERT_TRUE(writer.NeedDump(90));

        EXPECT_CALL(*mockSegmentWriter, IsDirty()).WillOnce(Return(false));
    }
    {
        // test for offline
        IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.SetIsOnline(false);
        PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);

        MockOfflinePartitionWriter writer(options);
        OpenWriter(writer, mSchema, partitionData);
        MockSingleSegmentWriter* mockSegmentWriter = new MockSingleSegmentWriter(mSchema, options);
        EXPECT_CALL(*mockSegmentWriter, IsDirty()).WillOnce(Return(false));
        SegmentWriterPtr segmentWriter(mockSegmentWriter);
        writer.mSegmentWriter = segmentWriter;

        { // EstimateMaxMemoryUseOfCurrentSegment reach (quota * ratio)
            size_t quota = 100;
            size_t quotaMultiRatio = (size_t)(quota * OfflinePartitionWriter::MEMORY_USE_RATIO);
            EXPECT_CALL(writer, EstimateMaxMemoryUseOfCurrentSegment()).WillOnce(Return(quotaMultiRatio + 1));
            ASSERT_TRUE(writer.NeedDump(quota));
        }
        {
            // EstimateMaxMemoryUseOfCurrentSegment + ResourceMemoryUse
            // reach quota after clean
            EXPECT_CALL(writer, EstimateMaxMemoryUseOfCurrentSegment()).WillOnce(Return(30));
            EXPECT_CALL(writer, GetResourceMemoryUse()).WillOnce(Return(100)).WillOnce(Return(90));
            EXPECT_CALL(writer, CleanResource()).WillOnce(Return());
            ASSERT_TRUE(writer.NeedDump(100));
        }
        {
            // EstimateMaxMemoryUseOfCurrentSegment + ResourceMemoryUse
            // not reach quota after clean
            EXPECT_CALL(writer, EstimateMaxMemoryUseOfCurrentSegment()).WillOnce(Return(30));
            EXPECT_CALL(writer, GetResourceMemoryUse()).WillOnce(Return(100)).WillOnce(Return(30));
            EXPECT_CALL(writer, CleanResource()).WillOnce(Return());
            ASSERT_FALSE(writer.NeedDump(100));
        }
        {
            // ResourceMemoryUse reach quota after clean
            EXPECT_CALL(writer, EstimateMaxMemoryUseOfCurrentSegment()).WillOnce(Return(30));
            EXPECT_CALL(writer, GetResourceMemoryUse()).WillOnce(Return(120)).WillOnce(Return(100));
            EXPECT_CALL(writer, CleanResource()).WillOnce(Return());
            ASSERT_THROW(writer.NeedDump(100), util::OutOfMemoryException);
        }
    }
}

void OfflinePartitionWriterTest::TestEmptyData()
{
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    auto memController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam param(options, mSchema, memController, dumpContainer, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());

    OpenWriter(writer, mSchema, partitionData);
    writer.Close();

    Version latestVersion;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), latestVersion, INVALID_VERSIONID);
    ASSERT_EQ(INVALID_VERSIONID, latestVersion.GetVersionId());
}

void OfflinePartitionWriterTest::TestDumpSegment()
{
    string field = "string1:string;";
    string index = "index1:string:string1;"
                   "pk:primarykey64:string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");

    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    auto memController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam param(options, schema, memController, dumpContainer, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello";
    NormalDocumentPtr document = DocumentCreator::CreateNormalDocument(schema, docString);
    document->SetTimestamp(10);
    auto locator = indexlibv2::framework::Locator(0, 1000);
    document->SetLocator(locator);

    ASSERT_TRUE(writer.BuildDocument(document));

    PartitionModifierPtr oldModifier = writer.mModifier;
    InMemorySegmentPtr oldInMemSeg = writer.mInMemorySegment;
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(rootDirectory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(rootDirectory->IsExist("segment_0_level_0/segment_info"));

    SegmentInfo segInfo;
    segInfo.Load(rootDirectory->GetDirectory("segment_0_level_0", true));
    ASSERT_EQ((uint32_t)1, segInfo.docCount);
    ASSERT_EQ(locator.Serialize(), segInfo.GetLocator().Serialize());
    ASSERT_EQ(10, segInfo.timestamp);

    ASSERT_TRUE(oldModifier.get() != writer.mModifier.get());
    ASSERT_EQ((uint32_t)0, writer.mSegmentInfo->docCount);

    oldModifier.reset();
    oldInMemSeg.reset();
    writer.mModifier.reset();
    partitionData.reset();
}

void OfflinePartitionWriterTest::TestGetLastFlushedLocator()
{
    string field = "string1:string;";
    string index = "index1:string:string1;"
                   "pk:primarykey64:string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");

    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);
    FlushedLocatorContainerPtr flushedLocatorContainer(new FlushedLocatorContainer(10));
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container, flushedLocatorContainer);
    auto memController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam param(options, schema, memController, container, counterMap, pluginManager, metricProvider,
                                 document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());
    OpenWriter(writer, schema, partitionData);

    EXPECT_FALSE(flushedLocatorContainer->GetLastFlushedLocator().IsValid());
    string docString = "cmd=add,string1=hello";
    NormalDocumentPtr document = DocumentCreator::CreateNormalDocument(schema, docString);
    document->SetTimestamp(10);
    document::Locator locator(indexlibv2::framework::Locator(0, 1000).Serialize());
    document->SetLocator(indexlibv2::framework::Locator(0, 1000));

    ASSERT_TRUE(writer.BuildDocument(document));
    EXPECT_FALSE(flushedLocatorContainer->GetLastFlushedLocator().IsValid());
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    document::Locator flushedLocator;
    while (!flushedLocator.IsValid()) {
        flushedLocator = flushedLocatorContainer->GetLastFlushedLocator();
    }
    EXPECT_EQ(locator, flushedLocator);

    document::Locator newLocator(indexlibv2::framework::Locator(0, 2000).Serialize());
    document->SetLocator(indexlibv2::framework::Locator(0, 2000));
    ASSERT_TRUE(writer.BuildDocument(document));
    EXPECT_EQ(locator, flushedLocatorContainer->GetLastFlushedLocator());

    writer.DumpSegment();
    flushedLocator = locator;
    while (flushedLocator == locator) {
        flushedLocator = flushedLocatorContainer->GetLastFlushedLocator();
    }
    EXPECT_EQ(newLocator, flushedLocator);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
}

void OfflinePartitionWriterTest::TestCleanResourceBeforeDumpSegment()
{
    // TODO: move
    // string field = "string1:string;";
    // string index = "index1:string:string1;"
    //                "pk:primarykey64:string1";
    // string attribute = "string1";
    // IndexPartitionSchemaPtr schema =
    //     SchemaMaker::MakeSchema(field, index, attribute, "");

    // IndexPartitionOptions options =
    // util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam()); options.SetIsOnline(false);
    // MockOfflinePartitionWriter writer(options);
    // DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    // PartitionDataPtr partitionData =
    //     PartitionDataCreator::CreateBuildingPartitionData(
    //             GET_FILE_SYSTEM(), schema, options,
    //             util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
    //             dumpContainer);
    // OpenWriter(writer, schema, partitionData);

    // string docString = "cmd=add,string1=hello";
    // DocumentPtr document = DocumentCreator::CreateNormalDocument(schema, docString);
    // writer.BuildDocument(document);
    // EXPECT_CALL(writer, CleanResource())
    //     .Times(1);
    // writer.DumpSegment();
}

void OfflinePartitionWriterTest::TestDumpSegmentWithOnlyDeleteDoc()
{
    string field = "string1:string;";
    string index = "index1:string:string1;"
                   "pk:primarykey64:string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");

    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    auto memController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam param(options, schema, memController, dumpContainer, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());
    OpenWriter(writer, schema, partitionData);

    string docStrings = "cmd=add,string1=hello;"
                        "cmd=delete,string1=hello;";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docStrings);
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_TRUE(writer.BuildDocument(docs[1]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(rootDirectory->IsExist("segment_1_level_0"));
    ASSERT_TRUE(rootDirectory->IsExist("segment_1_level_0/segment_info"));

    SegmentInfo segInfo;
    segInfo.Load(rootDirectory->GetDirectory("segment_1_level_0", true));
    ASSERT_EQ((uint32_t)0, segInfo.docCount);

    ASSERT_TRUE(rootDirectory->IsExist("segment_1_level_0/deletionmap/data_0"));
}

void OfflinePartitionWriterTest::TestDumpSegmentWithOnlyUpdateDoc()
{
    string field = "string1:string;price:int32";
    string index = "index1:string:string1;"
                   "pk:primarykey64:string1";
    string attribute = "price";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    auto memController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam param(options, schema, memController, dumpContainer, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());
    OpenWriter(writer, schema, partitionData);

    string docStrings = "cmd=add,string1=hello,price=10;"
                        "cmd=update_field,string1=hello,price=11";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docStrings);
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_TRUE(writer.BuildDocument(docs[1]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(rootDirectory->IsExist("segment_1_level_0"));
    ASSERT_TRUE(rootDirectory->IsExist("segment_1_level_0/segment_info"));

    SegmentInfo segInfo;
    segInfo.Load(rootDirectory->GetDirectory("segment_1_level_0", true));
    ASSERT_EQ((uint32_t)0, segInfo.docCount);

    ASSERT_TRUE(rootDirectory->IsExist("segment_1_level_0/attribute/price/1_0.patch"));
}

void OfflinePartitionWriterTest::TestAddDocument()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    string field = "string1:string;";
    string index = "pk:primarykey64:string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello";

    NormalDocumentPtr document = DocumentCreator::CreateNormalDocument(schema, docString);
    document->SetLocator(indexlibv2::framework::Locator(0, 100));
    document->SetTimestamp(10);
    ASSERT_TRUE(writer.BuildDocument(document));

    document::Locator locator(indexlibv2::framework::Locator(0, 1000).Serialize());
    document->SetLocator(indexlibv2::framework::Locator(0, 1000));
    document->SetTimestamp(20);
    ASSERT_TRUE(writer.BuildDocument(document));

    ASSERT_EQ((uint32_t)2, writer.mSegmentInfo->docCount);
    ASSERT_EQ((int64_t)20, writer.mSegmentInfo->timestamp);
    ASSERT_EQ(locator.ToString(), writer.mSegmentInfo->GetLocator().Serialize());

    InMemorySegmentPtr inMemorySegment = partitionData->GetInMemorySegment();
    InMemDeletionMapReaderPtr inMemDeletionMapReader = inMemorySegment->GetSegmentReader()->GetInMemDeletionMapReader();
    // delay remove dup doc to endsegment when offline build
    ASSERT_FALSE(inMemDeletionMapReader->IsDeleted(0));
    ASSERT_FALSE(inMemDeletionMapReader->IsDeleted(1));

    writer.Close();
}

void OfflinePartitionWriterTest::TestBuildWithOperationWriter()
{
    string field = "string1:string;";
    string index = "pk:primarykey64:string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    string docString = "cmd=add,string1=hello";
    NormalDocumentPtr document = DocumentCreator::CreateNormalDocument(schema, docString);
    document->SetLocator(indexlibv2::framework::Locator(0, 100));
    document->SetTimestamp(10);
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    { // test offline
        options.SetIsOnline(false);
        PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
        DumpSegmentContainerPtr container(new DumpSegmentContainer);
        OfflinePartitionWriter writer(options, container);
        OpenWriter(writer, schema, partitionData);
        ASSERT_TRUE(writer.BuildDocument(document));
        OperationWriterPtr opWriter = writer.mOperationWriter;
        ASSERT_FALSE(opWriter);
        size_t expectMemUse = writer.mSegmentWriter->GetTotalMemoryUse() + writer.mModifier->GetTotalMemoryUse();
        ASSERT_EQ(expectMemUse, writer.GetTotalMemoryUse());
    }
    { // test online
        options.SetIsOnline(true);
        PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
        DumpSegmentContainerPtr container(new DumpSegmentContainer);
        OfflinePartitionWriter writer(options, container);
        OpenWriter(writer, schema, partitionData);
        ASSERT_TRUE(writer.BuildDocument(document));
        OperationWriterPtr opWriter = writer.mOperationWriter;
        ASSERT_TRUE(opWriter);
        ASSERT_EQ((size_t)1, opWriter->Size());
        size_t expectMemUse = writer.mSegmentWriter->GetTotalMemoryUse() + writer.mModifier->GetTotalMemoryUse() +
                              writer.mOperationWriter->GetTotalMemoryUse();
        ASSERT_EQ(expectMemUse, writer.GetTotalMemoryUse());
    }
}

void OfflinePartitionWriterTest::TestUpdateDocumentInBuildingSegment()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=update_field,string1=hello,price=20,ts=20;"
                       "cmd=delete,string1=hello;"
                       "cmd=update_field,string1=hello,price=30,ts=20;";

    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);
    document::Locator locator(indexlibv2::framework::Locator(0, 100).Serialize());
    docs[1]->SetLocator(indexlibv2::framework::Locator(0, 100));

    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    ASSERT_TRUE(writer.BuildDocument(docs[1]));

    ASSERT_EQ((uint32_t)1, writer.mSegmentInfo->docCount);
    ASSERT_EQ((int64_t)20, writer.mSegmentInfo->timestamp);
    ASSERT_EQ(locator.ToString(), writer.mSegmentInfo->GetLocator().Serialize());

    InMemorySegmentPtr inMemorySegment = partitionData->GetInMemorySegment();
    AttributeSegmentReaderPtr attrSegReader = inMemorySegment->GetSegmentReader()->GetAttributeSegmentReader("price");
    int32_t value;
    bool isNull = false;
    autil::mem_pool::Pool pool;
    auto ctx = attrSegReader->CreateReadContextPtr(&pool);
    ASSERT_TRUE(attrSegReader->Read(0, ctx, (uint8_t*)&value, sizeof(int32_t), isNull));
    ASSERT_EQ((int32_t)20, value);

    // test update deleted document
    ASSERT_TRUE(writer.BuildDocument(docs[2]));
    ASSERT_FALSE(writer.BuildDocument(docs[3]));
    writer.Close();
}

void OfflinePartitionWriterTest::TestUpdateDocumentInBuiltSegment()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=update_field,string1=hello,price=20,ts=20;"
                       "cmd=delete,string1=hello;"
                       "cmd=update_field,string1=hello,price=30,ts=20;";

    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_TRUE(writer.BuildDocument(docs[1]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(rootDirectory->IsExist("segment_1_level_0/attribute/price/1_0.patch"));

    // test update deleted document
    ASSERT_TRUE(writer.BuildDocument(docs[2]));
    ASSERT_FALSE(writer.BuildDocument(docs[3]));
    writer.Close();
}

void OfflinePartitionWriterTest::TestUpdateDocumentNotExist()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=update_field,string1=non_exist,price=20,ts=20";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_FALSE(writer.BuildDocument(docs[1]));
    writer.Close();
}

void OfflinePartitionWriterTest::TestUpdateDocumentNoPkSchema()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string noPkIndex = "string1:string:string1";
    string attribute = "price";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionSchemaPtr noPkSchema = SchemaMaker::MakeSchema(field, noPkIndex, attribute, "");

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, noPkSchema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=update_field,string1=hello,price=20,ts=20";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, noPkSchema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_FALSE(writer.Validate(docs[1]));
    writer.Close();
}

void OfflinePartitionWriterTest::TestDeleteDocumentInBuildingSegment()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=delete,string1=hello,ts=20";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);
    document::Locator locator(indexlibv2::framework::Locator(0, 100).Serialize());
    docs[1]->SetLocator(indexlibv2::framework::Locator(0, 100));

    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    ASSERT_TRUE(writer.BuildDocument(docs[1]));
    ASSERT_FALSE(writer.BuildDocument(docs[1])); // deplicate remove

    ASSERT_EQ((uint32_t)1, writer.mSegmentInfo->docCount);
    ASSERT_EQ((int64_t)20, writer.mSegmentInfo->timestamp);
    ASSERT_EQ(locator.ToString(), writer.mSegmentInfo->GetLocator().Serialize());

    InMemorySegmentPtr inMemorySegment = partitionData->GetInMemorySegment();
    InMemDeletionMapReaderPtr inMemDeletionMapReader = inMemorySegment->GetSegmentReader()->GetInMemDeletionMapReader();
    ASSERT_TRUE(inMemDeletionMapReader->IsDeleted(0));
    writer.Close();

    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist("segment_0_level_0/deletionmap/data_0"));
}

void OfflinePartitionWriterTest::TestDeleteDocumentInBuiltSegment()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=delete,string1=hello,ts=20";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_TRUE(writer.BuildDocument(docs[1]));
    ASSERT_FALSE(writer.BuildDocument(docs[1])); // deplicate remove
    writer.Close();

    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist("segment_1_level_0/deletionmap/data_0"));
}

void OfflinePartitionWriterTest::TestDeleteDocumentNotExist()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=delete,string1=not_exist,ts=20";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_FALSE(writer.BuildDocument(docs[1]));
    writer.Close();
    ASSERT_FALSE(GET_PARTITION_DIRECTORY()->IsExist("segment_1_level_0"));
}

void OfflinePartitionWriterTest::TestDeleteDocumentNoPkSchema()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string noPkIndex = "string1:string:string1";
    string attribute = "price";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionSchemaPtr noPkSchema = SchemaMaker::MakeSchema(field, noPkIndex, attribute, "");

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);

    OpenWriter(writer, noPkSchema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=delete,string1=hello,ts=20";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, noPkSchema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_FALSE(writer.BuildDocument(docs[1]));
    writer.Close();
}

void OfflinePartitionWriterTest::TestGetFileSystemMemoryUse()
{
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);

    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;
    auto fs = FileSystemCreator::Create("TestGetFileSystemMemoryUse", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    auto memController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam partParam(options, mSchema, memController, dumpContainer, counterMap, pluginManager,
                                     metricProvider, document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(partParam, fs);
    OpenWriter(writer, mSchema, partitionData);

    unique_ptr<char[]> buffer5B(new char[5]);
    unique_ptr<char[]> buffer7B(new char[7]);
    DirectoryPtr rootDirectory = Directory::Get(fs);
    DirectoryPtr inMemDirectory = rootDirectory->MakeDirectory("in_mem_dir", DirectoryOption::Mem());
    WriterOption param;
    param.copyOnDump = true;
    FileWriterPtr fileWriter = inMemDirectory->CreateFileWriter("dump_file", param);
    fileWriter->Write(buffer5B.get(), 5).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    // fileNode in cache : 5 bytes; Cloned fileNode in FlushOperation: 5 bytes;
    ASSERT_EQ((size_t)10, writer.GetFileSystemMemoryUse());

    FileWriterPtr sliceFileWriter = rootDirectory->CreateFileWriter("slice_file", WriterOption::Slice(10, 1));
    sliceFileWriter->Write(buffer7B.get(), 7).GetOrThrow();
    ASSERT_EQ(FSEC_OK, sliceFileWriter->Close());
    ASSERT_EQ((size_t)17, writer.GetFileSystemMemoryUse());

    // sync will clean cache space of the dumping fileNode： dump_file
    rootDirectory->Sync(true);
    ASSERT_EQ((size_t)7, writer.GetFileSystemMemoryUse());
}

FileReaderPtr OfflinePartitionWriterTest::CreateDataFileReader(const DirectoryPtr& directory, int64_t dataSize)
{
    FileWriterPtr fileWriter = directory->CreateFileWriter("data_file");
    string value(dataSize, 'a');
    fileWriter->Write(value.data(), value.size()).GetOrThrow();
    fileWriter->Close().GetOrThrow();
    return directory->CreateFileReader("data_file", FSOT_MEM);
}

void OfflinePartitionWriterTest::InnerTestOpen(const IndexPartitionSchemaPtr& schema,
                                               const IndexPartitionOptions& options)
{
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    auto memController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam param(options, schema, memController, dumpContainer, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());
    OpenWriter(writer, schema, partitionData);

    ASSERT_TRUE(writer.mSchema);
    ASSERT_TRUE(writer.mPartitionData);
    ASSERT_TRUE(writer.mDocRewriteChain);
    ASSERT_TRUE(writer.mInMemorySegment);
    ASSERT_TRUE(writer.mSegmentWriter);
    ASSERT_TRUE(writer.mSegmentInfo);

    const SegmentData& segData = writer.mInMemorySegment->GetSegmentData();
    if (options.IsOffline()) {
        ASSERT_EQ((segmentid_t)0, segData.GetSegmentId());
    } else {
        ASSERT_EQ(RealtimeSegmentDirectory::ConvertToRtSegmentId(0), segData.GetSegmentId());
    }

    if (schema->GetIndexSchema()->HasPrimaryKeyIndex()) {
        ASSERT_TRUE(writer.mModifier);
    } else {
        ASSERT_FALSE(writer.mModifier);
    }
}

void OfflinePartitionWriterTest::OpenWriter(OfflinePartitionWriter& writer, const IndexPartitionSchemaPtr& schema,
                                            const PartitionDataPtr& partitionData)
{
    partitionData->CreateNewSegment();
    PartitionModifierPtr modifier = PartitionModifierCreator::CreateOfflineModifier(schema, partitionData, true, false);
    writer.Open(schema, partitionData, modifier);
}

void OfflinePartitionWriterTest::ReOpenNewSegment(OfflinePartitionWriter& writer, const IndexPartitionSchemaPtr& schema,
                                                  const PartitionDataPtr& partitionData)
{
    partitionData->CreateNewSegment();
    PartitionModifierPtr modifier = PartitionModifierCreator::CreateOfflineModifier(schema, partitionData, true, false);
    writer.ReOpenNewSegment(modifier);
}

void OfflinePartitionWriterTest::TestKVTable()
{
    // prepare schema and options
    string field = "key:int64;value:uint64;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    ASSERT_TRUE(schema);
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(false);

    // create partition data
    // MAX_MEMORY_USE_RESERVE_MIN = 1024
    // (16[Heaader] + 16[KV] * (2[Bucket] * 2 + 2[SpecialBucket]))
    uint64_t leftQuota = (1024 + 128) * 2;
    const IFileSystemPtr& fileSystem = GET_FILE_SYSTEM();
    const util::PartitionMemoryQuotaControllerPtr& memController =
        util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::QuotaControlPtr buildMemoryQuotaControler(new util::QuotaControl(leftQuota));
    memController->SetBuildMemoryQuotaControler(buildMemoryQuotaControler);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam param(options, schema, memController, dumpContainer, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, fileSystem);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    // test
    string docString = "cmd=add,key=10,value=1,ts=20,locator=1";
    auto document = DocumentCreator::CreateKVDocument(schema, docString,
                                                      /*defaultRegionName*/ "");
    ASSERT_TRUE(writer.BuildDocument(document));
    ASSERT_EQ(0, writer.EstimateMaxMemoryUseOfCurrentSegment());
    ASSERT_EQ(128, writer.GetFileSystemMemoryUse());
    ASSERT_FALSE(writer.NeedDump(leftQuota));

    ASSERT_EQ((uint32_t)1, writer.mSegmentInfo->docCount);
    ASSERT_EQ((int64_t)20, writer.mSegmentInfo->timestamp);
    ASSERT_EQ(indexlibv2::framework::Locator(0, 1).Serialize(), writer.mSegmentInfo->GetLocator().Serialize())
        << writer.mSegmentInfo->GetLocator().Serialize();

    docString = "cmd=add,key=20,value=2,ts=30,locator=2";
    document = DocumentCreator::CreateKVDocument(schema, docString, "");
    ASSERT_TRUE(writer.BuildDocument(document));
    ASSERT_EQ(0, writer.EstimateMaxMemoryUseOfCurrentSegment());
    ASSERT_EQ(128, writer.GetFileSystemMemoryUse());
    ASSERT_TRUE(writer.NeedDump(leftQuota));

    ASSERT_EQ((uint32_t)2, writer.mSegmentInfo->docCount);
    ASSERT_EQ((int64_t)30, writer.mSegmentInfo->timestamp);
    ASSERT_EQ(indexlibv2::framework::Locator(0, 2).Serialize(), writer.mSegmentInfo->GetLocator().Serialize())
        << writer.mSegmentInfo->GetLocator().Serialize();
    writer.Close();
}

void OfflinePartitionWriterTest::TestFailOverWithTemperature()
{
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetEnablePackageFile(true);
    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);

    string rootDir = GET_TEMP_DATA_PATH();
    PartitionStateMachine psm;
    uint64_t currentTime = Timer::GetCurrentTime();
    string docStrings = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 10) +
                        ";" // hot
                        "cmd=add,pk=2,status=1,time=" +
                        StringUtil::toString(currentTime - 5000) + ";";
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, rootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docStrings, "", ""));
    string incDocString = "cmd=add,pk=3,status=1,time=" + StringUtil::toString(currentTime - 1000000) +
                          ";" // hot
                          "cmd=add,pk=4,status=0,time=" +
                          StringUtil::toString(currentTime - 200000000) + ";"; // cold
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
    // remover version.2
    string version = rootDir + "version.2";
    auto ret = FslibWrapper::DeleteFile(version, DeleteOption::NoFence(false)).Code();
    ASSERT_EQ(FSEC_OK, ret);
    string incDocString2 = "cmd=add,pk=5,status=1,time=" + StringUtil::toString(currentTime - 1000000) + ";";

    PartitionStateMachine newPsm;
    INDEXLIB_TEST_TRUE(newPsm.Init(schema, options, rootDir));
    INDEXLIB_TEST_TRUE(newPsm.Transfer(BUILD_INC_NO_MERGE, incDocString2, "", ""));
    Version newVersion;
    VersionLoader::GetVersionS(rootDir, newVersion, 2);
    ASSERT_EQ(newVersion.GetSegmentVector().size(), newVersion.GetSegTemperatureMetas().size());
}

}} // namespace indexlib::partition
