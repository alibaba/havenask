#include "indexlib/partition/test/online_partition_writer_unittest.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/ttl_decoder.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OnlinePartitionWriterTest);

OnlinePartitionWriterTest::OnlinePartitionWriterTest() {}

OnlinePartitionWriterTest::~OnlinePartitionWriterTest() {}

void OnlinePartitionWriterTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    string field = "string1:string;string2:string;price:int32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void OnlinePartitionWriterTest::CaseTearDown() {}

void OnlinePartitionWriterTest::TestSimpleProcess()
{
    IndexPartitionOptions options;
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OnlinePartitionWriter writer(options, container);

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);
    OpenWriter(writer, mSchema, options, partitionData);
    ASSERT_TRUE(writer.mWriter);
    ASSERT_EQ(INVALID_TIMESTAMP, writer.mRtFilterTimestamp);

    writer.mRtFilterTimestamp = 2;
    string docString = "cmd=add,string1=hello,price=4,ts=1;"
                       "cmd=add,string1=hello,price=4,ts=2;"
                       "cmd=delete,string1=hello,price=5,ts=2;"
                       "cmd=add,string1=hello,price=4,ts=3;"
                       "cmd=delete,string1=hello,price=5,ts=4;";

    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(mSchema, docString);
    ASSERT_FALSE(writer.BuildDocument(docs[0]));
    ASSERT_TRUE(writer.BuildDocument(docs[1]));
    ASSERT_TRUE(writer.BuildDocument(docs[2]));
    ASSERT_TRUE(writer.BuildDocument(docs[3]));
    ASSERT_TRUE(writer.BuildDocument(docs[4]));
    InMemorySegmentPtr inMemSegment = partitionData->GetInMemorySegment();
    ASSERT_TRUE(inMemSegment);
    OperationWriterPtr opWriter = DYNAMIC_POINTER_CAST(OperationWriter, inMemSegment->GetOperationWriter());
    ASSERT_TRUE(opWriter);
    ASSERT_EQ(size_t(4), opWriter->Size());
}

namespace {
class MockOnlinePartitionWriter : public OnlinePartitionWriter
{
public:
    MockOnlinePartitionWriter()
        : OnlinePartitionWriter(IndexPartitionOptions(), DumpSegmentContainerPtr(new DumpSegmentContainer))
    {
    }

    MOCK_METHOD(bool, CheckTimestamp, (const document::DocumentPtr& doc), (const, override));
};
DEFINE_SHARED_PTR(MockOnlinePartitionWriter);

class MockOfflinePartitionWriter : public OfflinePartitionWriter
{
public:
    MockOfflinePartitionWriter()
        : OfflinePartitionWriter(IndexPartitionOptions(), DumpSegmentContainerPtr(new DumpSegmentContainer))
    {
        mTTLDecoder.reset(new TTLDecoder(mSchema));
        mSegmentWriter.reset(new SingleSegmentWriter(mSchema, mOptions));
    }
    MOCK_METHOD(bool, PreprocessDocument, (const document::DocumentPtr& doc), (override));
    MOCK_METHOD(bool, AddDocument, (const document::DocumentPtr& doc), (override));
    MOCK_METHOD(bool, UpdateDocument, (const document::DocumentPtr& doc), (override));
    MOCK_METHOD(bool, RemoveDocument, (const document::DocumentPtr& doc), (override));
    MOCK_METHOD(void, DumpSegment, (), (override));
    MOCK_METHOD(bool, NeedDump, (size_t, const DocumentCollector*), (const, override));
};
DEFINE_SHARED_PTR(MockOfflinePartitionWriter);

class MockPartitionData : public OnDiskPartitionData
{
public:
    MockPartitionData() : OnDiskPartitionData(plugin::PluginManagerPtr()) {}

public:
    MOCK_METHOD(void, UpdatePartitionInfo, (), (override));
    MOCK_METHOD(void, CommitVersion, (), (override));
};
DEFINE_SHARED_PTR(MockPartitionData);

} // namespace

void OnlinePartitionWriterTest::TestClose()
{
    IndexPartitionOptions options;
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    OnlinePartitionWriter writer(options, dumpContainer);

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);
    OpenWriter(writer, mSchema, options, partitionData);
    writer.Close();
    ASSERT_TRUE(!writer.mPartitionData);
    ASSERT_TRUE(!writer.mWriter);
}

void OnlinePartitionWriterTest::TestOpenAfterClose()
{
    IndexPartitionOptions options;
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    OnlinePartitionWriter writer(options, dumpContainer);

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);
    OpenWriter(writer, mSchema, options, partitionData);
    string docString = "cmd=add,string1=hello,price=4,ts=1;"
                       "cmd=add,string1=hello,price=4,ts=2;";

    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(mSchema, docString);
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    writer.Close();
    OpenWriter(writer, mSchema, options, partitionData);
    ASSERT_TRUE(writer.BuildDocument(docs[1]));
}

void OnlinePartitionWriterTest::TestAddDocument()
{
    MockOnlinePartitionWriterPtr onlineWriter(new MockOnlinePartitionWriter);
    MockOfflinePartitionWriterPtr offlineWriter(new MockOfflinePartitionWriter());
    MockPartitionDataPtr partitionData(new MockPartitionData);
    onlineWriter->mWriter = offlineWriter;
    onlineWriter->mPartitionData = partitionData;
    DocumentPtr doc(new NormalDocument);
    doc->SetDocOperateType(ADD_DOC);
    {
        // check timestamp fail
        EXPECT_CALL(*onlineWriter, CheckTimestamp(_)).WillOnce(Return(false));

        EXPECT_CALL(*offlineWriter, AddDocument(_)).Times(0);
        EXPECT_CALL(*partitionData, UpdatePartitionInfo()).Times(0);
        ASSERT_TRUE(!onlineWriter->BuildDocument(doc));
    }
    {
        // check timestamp ok, add operation ok, offlineWriter add fail
        EXPECT_CALL(*onlineWriter, CheckTimestamp(_)).WillOnce(Return(true));
        EXPECT_CALL(*offlineWriter, PreprocessDocument(_)).WillOnce(Return(true));
        EXPECT_CALL(*offlineWriter, AddDocument(_)).WillOnce(Return(false));
        EXPECT_CALL(*partitionData, UpdatePartitionInfo()).Times(0);
        ASSERT_FALSE(onlineWriter->BuildDocument(doc));
    }
    {
        // normal add document
        EXPECT_CALL(*onlineWriter, CheckTimestamp(_)).WillOnce(Return(true));
        EXPECT_CALL(*offlineWriter, PreprocessDocument(_)).WillOnce(Return(true));
        EXPECT_CALL(*offlineWriter, AddDocument(_)).WillOnce(Return(true));
        EXPECT_CALL(*partitionData, UpdatePartitionInfo()).Times(1);
        ASSERT_TRUE(onlineWriter->BuildDocument(doc));
    }
}

void OnlinePartitionWriterTest::TestUpdateDocument()
{
    MockOnlinePartitionWriterPtr onlineWriter(new MockOnlinePartitionWriter);
    MockOfflinePartitionWriterPtr offlineWriter(new MockOfflinePartitionWriter);
    MockPartitionDataPtr partitionData(new MockPartitionData);
    onlineWriter->mWriter = offlineWriter;
    onlineWriter->mPartitionData = partitionData;
    DocumentPtr doc(new NormalDocument);
    doc->SetDocOperateType(UPDATE_FIELD);
    {
        // check timestamp fail
        EXPECT_CALL(*onlineWriter, CheckTimestamp(_)).WillOnce(Return(false));

        EXPECT_CALL(*offlineWriter, UpdateDocument(_)).Times(0);
        EXPECT_CALL(*partitionData, UpdatePartitionInfo()).Times(0);
        ASSERT_FALSE(onlineWriter->BuildDocument(doc));
    }
    {
        // normal update document
        EXPECT_CALL(*onlineWriter, CheckTimestamp(_)).WillOnce(Return(true));
        EXPECT_CALL(*offlineWriter, PreprocessDocument(_)).WillOnce(Return(true));
        EXPECT_CALL(*offlineWriter, UpdateDocument(_)).WillOnce(Return(true));
        EXPECT_CALL(*partitionData, UpdatePartitionInfo()).Times(1);
        ASSERT_TRUE(onlineWriter->BuildDocument(doc));
    }
}

void OnlinePartitionWriterTest::TestRemoveDocument()
{
    MockOnlinePartitionWriterPtr onlineWriter(new MockOnlinePartitionWriter);
    MockOfflinePartitionWriterPtr offlineWriter(new MockOfflinePartitionWriter);
    MockPartitionDataPtr partitionData(new MockPartitionData);
    onlineWriter->mWriter = offlineWriter;
    onlineWriter->mPartitionData = partitionData;
    DocumentPtr doc(new NormalDocument);
    doc->SetDocOperateType(DELETE_DOC);
    {
        // check timestamp fail
        EXPECT_CALL(*onlineWriter, CheckTimestamp(_)).WillOnce(Return(false));

        EXPECT_CALL(*offlineWriter, RemoveDocument(_)).Times(0);
        EXPECT_CALL(*partitionData, UpdatePartitionInfo()).Times(0);
        ASSERT_TRUE(!onlineWriter->BuildDocument(doc));
    }
    {
        // check timestamp ok, add operation ok, offlineWriter remove fail
        EXPECT_CALL(*onlineWriter, CheckTimestamp(_)).WillOnce(Return(true));
        EXPECT_CALL(*offlineWriter, PreprocessDocument(_)).WillOnce(Return(true));
        EXPECT_CALL(*offlineWriter, RemoveDocument(_)).WillOnce(Return(false));
        EXPECT_CALL(*partitionData, UpdatePartitionInfo()).Times(0);
        ASSERT_FALSE(onlineWriter->BuildDocument(doc));
    }
    {
        // normal remove document
        EXPECT_CALL(*onlineWriter, CheckTimestamp(_)).WillOnce(Return(true));
        EXPECT_CALL(*offlineWriter, PreprocessDocument(_)).WillOnce(Return(true));
        EXPECT_CALL(*offlineWriter, RemoveDocument(_)).WillOnce(Return(true));
        EXPECT_CALL(*partitionData, UpdatePartitionInfo()).Times(1);
        ASSERT_TRUE(onlineWriter->BuildDocument(doc));
    }
}

void OnlinePartitionWriterTest::TestDumpSegment()
{
    MockOnlinePartitionWriterPtr onlineWriter(new MockOnlinePartitionWriter);
    MockOfflinePartitionWriterPtr offlineWriter(new MockOfflinePartitionWriter);
    MockPartitionDataPtr partitionData(new MockPartitionData);
    onlineWriter->mWriter = offlineWriter;
    offlineWriter->mPartitionData = partitionData;

    EXPECT_CALL(*offlineWriter, DumpSegment()).WillOnce(Return());
    // EXPECT_CALL(*partitionData, CommitVersion()).Times(1); TODO:
    onlineWriter->DumpSegment();
}

void OnlinePartitionWriterTest::TestNeedDump()
{
    MockOnlinePartitionWriterPtr onlineWriter(new MockOnlinePartitionWriter);
    MockOfflinePartitionWriterPtr offlineWriter(new MockOfflinePartitionWriter);
    onlineWriter->mWriter = offlineWriter;

    EXPECT_CALL(*offlineWriter, NeedDump(1024, nullptr)).WillOnce(Return(true));
    ASSERT_TRUE(onlineWriter->NeedDump(1024, nullptr));
}

void OnlinePartitionWriterTest::OpenWriter(OnlinePartitionWriter& writer, const IndexPartitionSchemaPtr& schema,
                                           const IndexPartitionOptions& options, const PartitionDataPtr& partitionData)
{
    partitionData->CreateNewSegment();
    IndexPartitionReaderPtr reader(new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    reader->Open(partitionData);
    PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(schema, reader);
    writer.Open(schema, partitionData, modifier);
}
}} // namespace indexlib::partition
