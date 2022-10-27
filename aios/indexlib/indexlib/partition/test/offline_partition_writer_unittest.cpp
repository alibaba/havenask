#include "indexlib/partition/test/offline_partition_writer_unittest.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/document/document.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/index/in_memory_segment_reader.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OfflinePartitionWriterTest);

namespace
{
    class MockSingleSegmentWriter : public SingleSegmentWriter
    {
    public:
        MockSingleSegmentWriter(
                const IndexPartitionSchemaPtr& schema,
                const IndexPartitionOptions& options)
            : SingleSegmentWriter(schema, options)
        {}
        MOCK_METHOD1(AddDocument, bool(const DocumentPtr& doc));
        MOCK_METHOD0(GetInMemorySegmentModifier, InMemorySegmentModifierPtr());
        MOCK_METHOD0(CreateInMemSegmentReader, index::InMemorySegmentReaderPtr());
        MOCK_METHOD0(EndSegment, void());
        MOCK_CONST_METHOD0(GetTotalMemoryUse, size_t());
        MOCK_METHOD1(Dump, void(const DirectoryPtr& directory));
        MOCK_CONST_METHOD0(IsDirty, bool());
        MOCK_CONST_METHOD0(EstimateMaxMemoryUseOfCurrentSegment, size_t ());
    };

    class MockOfflinePartitionWriter : public OfflinePartitionWriter
    {
    public:
        MockOfflinePartitionWriter(
                const config::IndexPartitionOptions& options)
            : OfflinePartitionWriter(options,
                                     DumpSegmentContainerPtr(new DumpSegmentContainer))
        {}
        MOCK_CONST_METHOD0(EstimateMaxMemoryUseOfCurrentSegment, size_t ());
        MOCK_CONST_METHOD0(GetResourceMemoryUse, size_t ());
        MOCK_CONST_METHOD0(CleanResource, void ());

    };

    class MockPartitionModifier : public PatchModifier
    {
    public:
        MockPartitionModifier()
            : PatchModifier(IndexPartitionSchemaPtr()) {}
        ~MockPartitionModifier() {}

        MOCK_CONST_METHOD0(EstimateMaxMemoryUseOfCurrentSegment, size_t());
        MOCK_CONST_METHOD0(IsDirty, bool());
    };
}

OfflinePartitionWriterTest::OfflinePartitionWriterTest()
{
}

OfflinePartitionWriterTest::~OfflinePartitionWriterTest()
{
}

void OfflinePartitionWriterTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    string field = "string1:string;";
    string index = "index1:string:string1;";
    mSchema = SchemaMaker::MakeSchema(field, index, "", "");
}

void OfflinePartitionWriterTest::CaseTearDown()
{
}

void OfflinePartitionWriterTest::TestSimpleProcess()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, mSchema);
    partitionData->CreateNewSegment();
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, mSchema, partitionData);

    string docString = "cmd=add,string1=hello";
    NormalDocumentPtr document = DocumentCreator::CreateDocument(mSchema, docString);

    writer.AddDocument(document);
    writer.DumpSegment();
    ReOpenNewSegment(writer, mSchema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    writer.AddDocument(document);
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
        IndexPartitionOptions options;
        InnerTestOpen(mSchema, options);
    }

    {
        // offline mode open
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        InnerTestOpen(mSchema, options);
    }

    {
        // schema with pk, online
        string field = "string1:string;";
        string index = "index1:string:string1;"
                       "pk:primarykey64:string1";
        IndexPartitionSchemaPtr schema =
            SchemaMaker::MakeSchema(field, index, "", "");
        IndexPartitionOptions options;
        InnerTestOpen(mSchema, options);
    }

    {
        // schema with pk, offline
        string field = "string1:string;";
        string index = "index1:string:string1;"
                       "pk:primarykey64:string1";
        IndexPartitionSchemaPtr schema =
            SchemaMaker::MakeSchema(field, index, "", "");
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        InnerTestOpen(mSchema, options);
    }
}

void OfflinePartitionWriterTest::TestNeedDump()
{
    {
        //test no segment writer
        IndexPartitionOptions options;
        DumpSegmentContainerPtr container(new DumpSegmentContainer);
        OfflinePartitionWriter writer(options, container);
        ASSERT_FALSE(writer.NeedDump(1024));
    }

    {
        //test max doc count trigger dump
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetBuildConfig().maxDocCount = 5;

        PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
                    GET_FILE_SYSTEM(), options, mSchema);
        MockOfflinePartitionWriter writer(options);
        OpenWriter(writer, mSchema, partitionData);

        writer.mSegmentInfo->docCount = 5;
        ASSERT_TRUE(writer.NeedDump(1024));
    }

    {
        // test for online
        IndexPartitionOptions options;
        options.SetIsOnline(true);
        PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, mSchema);

        MockOfflinePartitionWriter writer(options);
        OpenWriter(writer, mSchema, partitionData);
        MockSingleSegmentWriter* mockSegmentWriter =
            new MockSingleSegmentWriter(mSchema, options);
        SegmentWriterPtr segmentWriter(mockSegmentWriter);
        writer.mSegmentWriter = segmentWriter;

        EXPECT_CALL(writer, EstimateMaxMemoryUseOfCurrentSegment())
            .WillRepeatedly(Return(100));
        ASSERT_FALSE(writer.NeedDump(200));
        ASSERT_TRUE(writer.NeedDump(90));

        EXPECT_CALL(*mockSegmentWriter, IsDirty())
            .WillOnce(Return(false));
    }
    {
        // test for offline
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, mSchema);

        MockOfflinePartitionWriter writer(options);
        OpenWriter(writer, mSchema, partitionData);
        MockSingleSegmentWriter* mockSegmentWriter =
            new MockSingleSegmentWriter(mSchema, options);
        EXPECT_CALL(*mockSegmentWriter, IsDirty())
            .WillOnce(Return(false));
        SegmentWriterPtr segmentWriter(mockSegmentWriter);
        writer.mSegmentWriter = segmentWriter;

        {        // EstimateMaxMemoryUseOfCurrentSegment reach (quota * ratio)
            size_t quota = 100;
            size_t quotaMultiRatio = (size_t)(quota * OfflinePartitionWriter::MEMORY_USE_RATIO);
            EXPECT_CALL(writer, EstimateMaxMemoryUseOfCurrentSegment())
                .WillOnce(Return(quotaMultiRatio + 1));
            ASSERT_TRUE(writer.NeedDump(quota));
        }
        {
            // EstimateMaxMemoryUseOfCurrentSegment + ResourceMemoryUse
            // reach quota after clean
            EXPECT_CALL(writer, EstimateMaxMemoryUseOfCurrentSegment())
                .WillOnce(Return(30));
            EXPECT_CALL(writer, GetResourceMemoryUse())
                .WillOnce(Return(100))
                .WillOnce(Return(90));
            EXPECT_CALL(writer, CleanResource())
                .WillOnce(Return());
            ASSERT_TRUE(writer.NeedDump(100));
        }
        {
            // EstimateMaxMemoryUseOfCurrentSegment + ResourceMemoryUse
            // not reach quota after clean
            EXPECT_CALL(writer, EstimateMaxMemoryUseOfCurrentSegment())
                .WillOnce(Return(30));
            EXPECT_CALL(writer, GetResourceMemoryUse())
                .WillOnce(Return(100))
                .WillOnce(Return(30));
            EXPECT_CALL(writer, CleanResource())
                .WillOnce(Return());
            ASSERT_FALSE(writer.NeedDump(100));
        }
        {
            // ResourceMemoryUse reach quota after clean
            EXPECT_CALL(writer, EstimateMaxMemoryUseOfCurrentSegment())
                .WillOnce(Return(30));
            EXPECT_CALL(writer, GetResourceMemoryUse())
                .WillOnce(Return(120))
                .WillOnce(Return(100));
            EXPECT_CALL(writer, CleanResource())
                .WillOnce(Return());
            ASSERT_THROW(writer.NeedDump(100), misc::OutOfMemoryException);
        }
    }
}

void OfflinePartitionWriterTest::TestEmptyData()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), mSchema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                dumpContainer);

    OpenWriter(writer, mSchema, partitionData);
    writer.Close();

    Version latestVersion;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(),
                              latestVersion, INVALID_VERSION);
    ASSERT_EQ(INVALID_VERSION, latestVersion.GetVersionId());
}

void OfflinePartitionWriterTest::TestDumpSegment()
{
    string field = "string1:string;";
    string index = "index1:string:string1;"
                   "pk:primarykey64:string1";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "", "");

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), schema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                dumpContainer);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello";
    NormalDocumentPtr document = DocumentCreator::CreateDocument(schema, docString);
    document->SetTimestamp(10);
    document::Locator locator("1000");
    document->SetLocator(locator);

    writer.AddDocument(document);

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
    ASSERT_EQ(locator, segInfo.locator);
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
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "", "");

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    FlushedLocatorContainerPtr flushedLocatorContainer(new FlushedLocatorContainer(10));
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container, flushedLocatorContainer);
    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), schema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                container);
    OpenWriter(writer, schema, partitionData);

    EXPECT_FALSE(flushedLocatorContainer->GetLastFlushedLocator().IsValid());
    string docString = "cmd=add,string1=hello";
    NormalDocumentPtr document = DocumentCreator::CreateDocument(schema, docString);
    document->SetTimestamp(10);
    document::Locator locator("1000");
    document->SetLocator(locator);

    writer.AddDocument(document);
    EXPECT_FALSE(flushedLocatorContainer->GetLastFlushedLocator().IsValid());
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    document::Locator flushedLocator;
    while(!flushedLocator.IsValid())
    {
        flushedLocator = flushedLocatorContainer->GetLastFlushedLocator();
    }
    EXPECT_EQ(locator, flushedLocator);

    document::Locator newLocator("2000");
    document->SetLocator(newLocator);
    writer.AddDocument(document);
    EXPECT_EQ(locator, flushedLocatorContainer->GetLastFlushedLocator());

    writer.DumpSegment();
    flushedLocator = locator;
    while(flushedLocator == locator)
    {
        flushedLocator = flushedLocatorContainer->GetLastFlushedLocator();
    }
    EXPECT_EQ(newLocator, flushedLocator);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
}

void OfflinePartitionWriterTest::TestCleanResourceBeforeDumpSegment()
{
    //TODO: move
    // string field = "string1:string;";
    // string index = "index1:string:string1;"
    //                "pk:primarykey64:string1";
    // string attribute = "string1";
    // IndexPartitionSchemaPtr schema =
    //     SchemaMaker::MakeSchema(field, index, attribute, "");

    // IndexPartitionOptions options;
    // options.SetIsOnline(false);
    // MockOfflinePartitionWriter writer(options);
    // DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    // PartitionDataPtr partitionData =
    //     PartitionDataCreator::CreateBuildingPartitionData(
    //             GET_FILE_SYSTEM(), schema, options,
    //             util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
    //             dumpContainer);
    // OpenWriter(writer, schema, partitionData);

    // string docString = "cmd=add,string1=hello";
    // DocumentPtr document = DocumentCreator::CreateDocument(schema, docString);
    // writer.AddDocument(document);
    // EXPECT_CALL(writer, CleanResource())
    //     .Times(1);
    // writer.DumpSegment();
}

void OfflinePartitionWriterTest::TestDumpSegmentWithOnlyDeleteDoc()
{
    string field = "string1:string;";
    string index = "index1:string:string1;"
                   "pk:primarykey64:string1";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "", "");

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), schema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                dumpContainer);
    OpenWriter(writer, schema, partitionData);

    string docStrings = "cmd=add,string1=hello;"
                        "cmd=delete,string1=hello;";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateDocuments(schema, docStrings);
    writer.AddDocument(docs[0]);
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    writer.RemoveDocument(docs[1]);
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
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), schema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                dumpContainer);
    OpenWriter(writer, schema, partitionData);

    string docStrings = "cmd=add,string1=hello,price=10;"
                        "cmd=update_field,string1=hello,price=11";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateDocuments(schema, docStrings);
    writer.AddDocument(docs[0]);
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    writer.UpdateDocument(docs[1]);
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
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);

    string field = "string1:string;";
    string index = "pk:primarykey64:string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello";
    document::Locator locator;

    NormalDocumentPtr document = DocumentCreator::CreateDocument(schema, docString);
    locator.SetLocator("100");
    document->SetLocator(locator);
    document->SetTimestamp(10);
    writer.AddDocument(document);

    locator.SetLocator("1000");
    document->SetLocator(locator);
    document->SetTimestamp(20);
    writer.AddDocument(document);

    ASSERT_EQ((uint32_t)2, writer.mSegmentInfo->docCount);
    ASSERT_EQ((int64_t)20, writer.mSegmentInfo->timestamp);
    ASSERT_EQ(locator, writer.mSegmentInfo->locator);

    InMemorySegmentPtr inMemorySegment = partitionData->GetInMemorySegment();
    InMemDeletionMapReaderPtr inMemDeletionMapReader =
        inMemorySegment->GetSegmentReader()->GetInMemDeletionMapReader();
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
    document::Locator locator;
    NormalDocumentPtr document = DocumentCreator::CreateDocument(schema, docString);
    locator.SetLocator("100");
    document->SetLocator(locator);
    document->SetTimestamp(10);
    config::IndexPartitionOptions options;
    {   // test offline
        options.SetIsOnline(false);
        PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
            GET_FILE_SYSTEM(), options, schema);
        DumpSegmentContainerPtr container(new DumpSegmentContainer);
        OfflinePartitionWriter writer(options, container);
        OpenWriter(writer, schema, partitionData);
        writer.AddDocument(document);
        OperationWriterPtr opWriter = writer.mOperationWriter;
        ASSERT_FALSE(opWriter);
        size_t expectMemUse = writer.mSegmentWriter->GetTotalMemoryUse()
                              + writer.mModifier->GetTotalMemoryUse();
        ASSERT_EQ(expectMemUse, writer.GetTotalMemoryUse());

    }
    { // test online
        options.SetIsOnline(true);
        PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
            GET_FILE_SYSTEM(), options, schema);
        DumpSegmentContainerPtr container(new DumpSegmentContainer);
        OfflinePartitionWriter writer(options, container);
        OpenWriter(writer, schema, partitionData);
        writer.AddDocument(document);
        OperationWriterPtr opWriter = writer.mOperationWriter;
        ASSERT_TRUE(opWriter);
        ASSERT_EQ((size_t)1, opWriter->Size());
        size_t expectMemUse = writer.mSegmentWriter->GetTotalMemoryUse()
                              + writer.mModifier->GetTotalMemoryUse()
                              + writer.mOperationWriter->GetTotalMemoryUse();
        ASSERT_EQ(expectMemUse, writer.GetTotalMemoryUse());
    }
}

void OfflinePartitionWriterTest::TestUpdateDocumentInBuildingSegment()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=update_field,string1=hello,price=20,ts=20;"
                       "cmd=delete,string1=hello;"
                       "cmd=update_field,string1=hello,price=30,ts=20;";

    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docString);
    document::Locator locator;
    locator.SetLocator("100");
    docs[1]->SetLocator(locator);

    writer.AddDocument(docs[0]);
    ASSERT_TRUE(writer.UpdateDocument(docs[1]));

    ASSERT_EQ((uint32_t)1, writer.mSegmentInfo->docCount);
    ASSERT_EQ((int64_t)20, writer.mSegmentInfo->timestamp);
    ASSERT_EQ(locator, writer.mSegmentInfo->locator);

    InMemorySegmentPtr inMemorySegment = partitionData->GetInMemorySegment();
    AttributeSegmentReaderPtr attrSegReader =
        inMemorySegment->GetSegmentReader()->GetAttributeSegmentReader("price");
    int32_t value;
    ASSERT_TRUE(attrSegReader->Read(0, (uint8_t*)&value, sizeof(int32_t)));
    ASSERT_EQ((int32_t)20, value);

    // test update deleted document
    ASSERT_TRUE(writer.RemoveDocument(docs[2]));
    ASSERT_FALSE(writer.UpdateDocument(docs[3]));
    writer.Close();
}

void OfflinePartitionWriterTest::TestUpdateDocumentInBuiltSegment()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=update_field,string1=hello,price=20,ts=20;"
                       "cmd=delete,string1=hello;"
                       "cmd=update_field,string1=hello,price=30,ts=20;";

    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docString);
    writer.AddDocument(docs[0]);
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_TRUE(writer.UpdateDocument(docs[1]));
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(rootDirectory->IsExist("segment_1_level_0/attribute/price/1_0.patch"));

    // test update deleted document
    ASSERT_TRUE(writer.RemoveDocument(docs[2]));
    ASSERT_FALSE(writer.UpdateDocument(docs[3]));
    writer.Close();
}

void OfflinePartitionWriterTest::TestUpdateDocumentNotExist()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=update_field,string1=non_exist,price=20,ts=20";
    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docString);
    writer.AddDocument(docs[0]);
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_FALSE(writer.UpdateDocument(docs[1]));
    writer.Close();
}

void OfflinePartitionWriterTest::TestUpdateDocumentNoPkSchema()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string noPkIndex = "string1:string:string1";
    string attribute = "price";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionSchemaPtr noPkSchema =
        SchemaMaker::MakeSchema(field, noPkIndex, attribute, "");

    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, noPkSchema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=update_field,string1=hello,price=20,ts=20";
    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docString);
    writer.AddDocument(docs[0]);
    writer.DumpSegment();
    ReOpenNewSegment(writer, noPkSchema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_FALSE(writer.UpdateDocument(docs[1]));
    writer.Close();
}

void OfflinePartitionWriterTest::TestDeleteDocumentInBuildingSegment()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=delete,string1=hello,ts=20";
    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docString);
    document::Locator locator;
    locator.SetLocator("100");
    docs[1]->SetLocator(locator);

    writer.AddDocument(docs[0]);
    ASSERT_TRUE(writer.RemoveDocument(docs[1]));
    ASSERT_FALSE(writer.RemoveDocument(docs[1])); // deplicate remove

    ASSERT_EQ((uint32_t)1, writer.mSegmentInfo->docCount);
    ASSERT_EQ((int64_t)20, writer.mSegmentInfo->timestamp);
    ASSERT_EQ(locator, writer.mSegmentInfo->locator);

    InMemorySegmentPtr inMemorySegment = partitionData->GetInMemorySegment();
    InMemDeletionMapReaderPtr inMemDeletionMapReader =
        inMemorySegment->GetSegmentReader()->GetInMemDeletionMapReader();
    ASSERT_TRUE(inMemDeletionMapReader->IsDeleted(0));
    writer.Close();

    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist("segment_0_level_0/deletionmap/data_0"));
}

void OfflinePartitionWriterTest::TestDeleteDocumentInBuiltSegment()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=delete,string1=hello,ts=20";
    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docString);
    writer.AddDocument(docs[0]);
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_TRUE(writer.RemoveDocument(docs[1]));
    ASSERT_FALSE(writer.RemoveDocument(docs[1])); // deplicate remove
    writer.Close();

    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist("segment_1_level_0/deletionmap/data_0"));
}

void OfflinePartitionWriterTest::TestDeleteDocumentNotExist()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");

    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    OpenWriter(writer, schema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=delete,string1=not_exist,ts=20";
    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docString);
    writer.AddDocument(docs[0]);
    writer.DumpSegment();
    ReOpenNewSegment(writer, schema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_FALSE(writer.RemoveDocument(docs[1]));
    writer.Close();
    ASSERT_FALSE(GET_PARTITION_DIRECTORY()->IsExist("segment_1_level_0"));
}

void OfflinePartitionWriterTest::TestDeleteDocumentNoPkSchema()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);

    string field = "string1:string;price:int32";
    string index = "pk:primarykey64:string1";
    string noPkIndex = "string1:string:string1";
    string attribute = "price";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionSchemaPtr noPkSchema =
        SchemaMaker::MakeSchema(field, noPkIndex, attribute, "");

    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, schema);
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);

    OpenWriter(writer, noPkSchema, partitionData);

    string docString = "cmd=add,string1=hello,price=10,ts=10;"
                       "cmd=delete,string1=hello,ts=20";
    vector<NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docString);
    writer.AddDocument(docs[0]);
    writer.DumpSegment();
    ReOpenNewSegment(writer, noPkSchema, partitionData);
    container->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    ASSERT_FALSE(writer.RemoveDocument(docs[1]));
    writer.Close();
}

void OfflinePartitionWriterTest::TestGetFileSystemMemoryUse()
{
    IndexPartitionOptions options;
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);

    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), mSchema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                dumpContainer);
    OpenWriter(writer, mSchema, partitionData);

    unique_ptr<char[]> buffer5B(new char[5]);
    unique_ptr<char[]> buffer7B(new char[7]);
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr inMemDirectory = rootDirectory->MakeInMemDirectory("in_mem_dir");
    FSWriterParam param;
    param.copyOnDump = true;
    FileWriterPtr fileWriter = inMemDirectory->CreateFileWriter("dump_file", param);
    fileWriter->Write(buffer5B.get(), 5);
    fileWriter->Close();
    // fileNode in cache : 5 bytes; Cloned fileNode in FlushOperation: 5 bytes;
    ASSERT_EQ((size_t)10, writer.GetFileSystemMemoryUse());

    SliceFilePtr sliceFile = rootDirectory->CreateSliceFile("slice_file",
            10, 1);
    SliceFileWriterPtr sliceFileWriter = sliceFile->CreateSliceFileWriter();
    sliceFileWriter->Write(buffer7B.get(), 7);
    sliceFileWriter->Close();
    ASSERT_EQ((size_t)17, writer.GetFileSystemMemoryUse());

    // sync will clean cache space of the dumping fileNode： dump_file
    rootDirectory->Sync(true);
    ASSERT_EQ((size_t)7, writer.GetFileSystemMemoryUse());
}

FileReaderPtr OfflinePartitionWriterTest::CreateDataFileReader(
        const DirectoryPtr& directory, int64_t dataSize)
{
    FileWriterPtr fileWriter = directory->CreateFileWriter("data_file");
    string value(dataSize, 'a');
    fileWriter->Write(value.data(), value.size());
    fileWriter->Close();
    return directory->CreateFileReader("data_file", FSOT_IN_MEM);
}


void OfflinePartitionWriterTest::InnerTestOpen(
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options)
{
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OfflinePartitionWriter writer(options, container);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), schema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                dumpContainer);
    OpenWriter(writer, schema, partitionData);

    ASSERT_TRUE(writer.mSchema);
    ASSERT_TRUE(writer.mPartitionData);
    ASSERT_TRUE(writer.mDocRewriteChain);
    ASSERT_TRUE(writer.mInMemorySegment);
    ASSERT_TRUE(writer.mSegmentWriter);
    ASSERT_TRUE(writer.mSegmentInfo);

    const SegmentData& segData = writer.mInMemorySegment->GetSegmentData();
    if (options.IsOffline())
    {
        ASSERT_EQ((segmentid_t)0, segData.GetSegmentId());
    }
    else
    {
        ASSERT_EQ(RealtimeSegmentDirectory::ConvertToRtSegmentId(0),
                    segData.GetSegmentId());
    }

    if (schema->GetIndexSchema()->HasPrimaryKeyIndex())
    {
        ASSERT_TRUE(writer.mModifier);
    }
    else
    {
        ASSERT_FALSE(writer.mModifier);
    }
}

void OfflinePartitionWriterTest::OpenWriter(
        OfflinePartitionWriter& writer,
        const IndexPartitionSchemaPtr& schema,
        const PartitionDataPtr& partitionData)
{
    partitionData->CreateNewSegment();
    PartitionModifierPtr modifier =
        PartitionModifierCreator::CreateOfflineModifier(schema, partitionData,
                true, false);
    writer.Open(schema, partitionData, modifier);
}

void OfflinePartitionWriterTest::ReOpenNewSegment(
        OfflinePartitionWriter& writer,
        const IndexPartitionSchemaPtr& schema,
        const PartitionDataPtr& partitionData)
{
    partitionData->CreateNewSegment();
    PartitionModifierPtr modifier =
        PartitionModifierCreator::CreateOfflineModifier(
                schema, partitionData, true, false);
    writer.ReOpenNewSegment(modifier);
}

IE_NAMESPACE_END(partition);
