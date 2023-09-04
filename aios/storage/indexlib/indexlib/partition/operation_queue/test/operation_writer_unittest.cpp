#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/in_mem_segment_operation_iterator.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/operation_queue/test/mock_operation.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;

using namespace indexlib::test;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::config;

namespace indexlib { namespace partition {
class OperationWriterTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    OperationWriterTest();
    ~OperationWriterTest();

    DECLARE_CLASS_NAME(OperationWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    void InnerTestDump(const std::string& docString, const config::IndexPartitionSchemaPtr& schema);

    void AddOperations(const OperationWriterPtr& opWriter, const std::string& docString, size_t opCount,
                       int64_t startTs);

    void AddOperationThreadFunc(const OperationWriterPtr& opWriter, const std::string& docString, size_t opCount,
                                int64_t startTs);

    void ReadOperationThreadFunc(const OperationWriterPtr& opWriter, size_t expectOpCount, int64_t startTs);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionSchemaPtr mMainSubSchema;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(partition, OperationWriterTest);
INSTANTIATE_TEST_CASE_P(OperationWriterTestMode, OperationWriterTest, testing::Values(false, true));

namespace {
class MockOperationWriter : public OperationWriter
{
public:
    MOCK_METHOD(OperationFactoryPtr, CreateOperationFactory, (const config::IndexPartitionSchemaPtr& schema),
                (override));
};
}; // namespace

OperationWriterTest::OperationWriterTest() {}

OperationWriterTest::~OperationWriterTest() {}

void OperationWriterTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32;string3:string:true:true";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price;string3";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");

    mMainSubSchema.reset(mSchema->Clone());
    string subdoc_field = "sub_string1:string;sub_string2:string:true:true;sub_price:uint32;";
    string subdoc_index = "sub_pk:primarykey64:sub_string1";
    string subdoc_attribute = "sub_string1;sub_string2;sub_price";
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(subdoc_field, subdoc_index, subdoc_attribute, "");
    mMainSubSchema->SetSubIndexPartitionSchema(subSchema);
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void OperationWriterTest::CaseTearDown() {}

TEST_P(OperationWriterTest, TestSimpleProcess)
{
    // test single docs
    string docString = "cmd=add,string1=hello,locator=-1,ts=4;"
                       "cmd=add,string1=hello0,locator=0,ts=3;"
                       "cmd=update_field,string1=hello,price=5,ts=6;";

    InnerTestDump(docString, mSchema);
    // test main-sub docs
    string mainSubDocString = "cmd=add,string1=hello,ts=4,sub_string1=hi^ho;"
                              "cmd=add,string1=hello1,ts=4,sub_string1=me^mo;"
                              "cmd=delete_sub,string1=hello1,ts=4,sub_string1=me;"
                              "cmd=update_field,string1=hello1,price=1,ts=4,sub_string1=me,sub_string2=new;";
    InnerTestDump(mainSubDocString, mMainSubSchema);
}

void OperationWriterTest::InnerTestDump(const string& docString, const IndexPartitionSchemaPtr& schema)
{
    tearDown();
    setUp();
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(schema, docString);

    OperationWriter operationWriter;
    operationWriter.Init(schema, index::DEFAULT_MAX_OPERATION_BLOCK_SIZE, GetParam());

    for (size_t i = 0; i < docs.size(); ++i) {
        operationWriter.AddOperation(docs[i]);
    }
    ASSERT_EQ(docs.size(), operationWriter.Size());
    for (size_t i = 0; i < operationWriter.mOpBlocks.size(); i++) {
        OperationBlockPtr opBlock = operationWriter.mOpBlocks[i];
        ASSERT_TRUE(opBlock->GetTotalMemoryUse() > 0);
    }

    operationWriter.Dump(GET_SEGMENT_DIRECTORY());
    DirectoryPtr opLogDir = GET_SEGMENT_DIRECTORY()->GetDirectory(OPERATION_DIR_NAME, true);
    ASSERT_TRUE(opLogDir);

    ASSERT_TRUE(opLogDir->IsExist(OPERATION_DATA_FILE_NAME));
    ASSERT_TRUE(opLogDir->IsExist(OPERATION_META_FILE_NAME));

    // for (size_t i = 0; i < operationWriter.mOpBlocks.size(); i++)
    // {
    //     OperationBlockPtr opBlock = operationWriter.mOpBlocks[i];
    //     ASSERT_EQ((size_t)0, opBlock->GetTotalMemoryUse());
    // }
}

TEST_P(OperationWriterTest, TestEstimateDumpSize)
{
    MockOperationFactory* mockOpFactory = new MockOperationFactory();
    OperationFactoryPtr opFactory(mockOpFactory);

    autil::mem_pool::Pool pool;
    vector<MockOperation*> mockOpVec;
    mockOpVec.push_back(IE_POOL_NEW_CLASS(&pool, MockOperation, 0));
    mockOpVec.push_back(IE_POOL_NEW_CLASS(&pool, MockOperation, 0));
    mockOpVec.push_back(IE_POOL_NEW_CLASS(&pool, MockOperation, 0));
    // OperationBasePtr operation1(mockOp1);

    NormalDocumentPtr doc(new NormalDocument);
    IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
    indexDoc->SetPrimaryKey("pk");
    doc->SetIndexDocument(indexDoc);

    MockOperationWriter opWriter;
    EXPECT_CALL(opWriter, CreateOperationFactory(_)).WillOnce(Return(opFactory));
    opWriter.Init(mSchema, 2, GetParam());
    const util::BuildResourceMetricsPtr& buildMetrics = opWriter.GetBuildResourceMetrics();

    EXPECT_CALL(*mockOpFactory, CreateOperation(_, _, _))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(mockOpVec[0]), Return(true)))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(mockOpVec[1]), Return(true)))
        .WillOnce(DoAll(::testing::SetArgPointee<2>(mockOpVec[2]), Return(true)));

    for (size_t i = 1; i <= 3; i++) {
        EXPECT_CALL(*(mockOpVec[i - 1]), GetSerializeSize()).WillOnce(Return(i * 10));
        opWriter.AddOperation(doc);
        EXPECT_EQ((int64_t)(1 + 8 + i * 10), buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE));
    }

    // expect size: (1 + 8 + 10) + (1 + 8 + 20) + (1 + 8 + 30) = 87 bytes
    // operation dumpsize : opType + timestamp + op serialize size
    ASSERT_EQ((size_t)87, buildMetrics->GetValue(util::BMT_DUMP_FILE_SIZE));
}

TEST_P(OperationWriterTest, TestDumpMetaFile)
{
    string docString = "cmd=update_field,string1=hello,price=5,string3=abcdefghigklmnopqrstuvwxyz0123456789,ts=6;";

    OperationWriterPtr opWriter(new OperationWriter());
    opWriter->Init(mSchema, 2, GetParam());
    AddOperations(opWriter, docString, 5, 0);

    opWriter->Dump(GET_SEGMENT_DIRECTORY());

    DirectoryPtr opLogDir = GET_SEGMENT_DIRECTORY()->GetDirectory(OPERATION_DIR_NAME, true);
    ASSERT_TRUE(opLogDir);
    string metaFileContent;
    opLogDir->Load(OPERATION_META_FILE_NAME, metaFileContent);

    OperationMeta operationMeta;
    operationMeta.InitFromString(metaFileContent);

    const OperationMeta::BlockMetaVec& blockMetaVec = operationMeta.GetBlockMetaVec();

    size_t blockCount = 3;
    ASSERT_EQ(blockCount, blockMetaVec.size());

    ASSERT_EQ((int64_t)0, blockMetaVec[0].minTimestamp);
    ASSERT_EQ((int64_t)1, blockMetaVec[0].maxTimestamp);
    ASSERT_EQ((uint32_t)2, blockMetaVec[0].operationCount);

    ASSERT_EQ((int64_t)2, blockMetaVec[1].minTimestamp);
    ASSERT_EQ((int64_t)3, blockMetaVec[1].maxTimestamp);
    ASSERT_EQ((uint32_t)2, blockMetaVec[1].operationCount);

    ASSERT_EQ((int64_t)4, blockMetaVec[2].minTimestamp);
    ASSERT_EQ((int64_t)4, blockMetaVec[2].maxTimestamp);
    ASSERT_EQ((uint32_t)1, blockMetaVec[2].operationCount);
}

void OperationWriterTest::AddOperations(const OperationWriterPtr& opWriter, const string& docString, size_t opCount,
                                        int64_t startTs)
{
    assert(opWriter);
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(mSchema, docString);
    for (size_t i = 1; i <= opCount; ++i) {
        doc->SetTimestamp(startTs++);
        opWriter->AddOperation(doc);
    }
}

TEST_P(OperationWriterTest, TestBug34063306)
{
    {
        OperationWriterPtr opWriter(new OperationWriter());
        opWriter->Init(mSchema, 5, GetParam());
        string docString = "cmd=update_field,string1=hello,price=5,ts=6;";
        AddOperations(opWriter, docString, 5, 1);
        InMemSegmentOperationIteratorPtr inMemSegOpIter = opWriter->CreateSegmentOperationIterator(0, 0, 0);
        ASSERT_TRUE(inMemSegOpIter);
        AddOperations(opWriter, docString, 3, 1);
        size_t opCount(0);
        while (inMemSegOpIter->HasNext()) {
            OperationBase* op = inMemSegOpIter->Next();
            ASSERT_TRUE(op);
            ++opCount;
        }

        ASSERT_EQ(size_t(5), opCount);
        auto opCursor = inMemSegOpIter->GetLastCursor();
        ASSERT_EQ(4, opCursor.pos);
    }
    {
        OperationWriterPtr opWriter(new OperationWriter());
        opWriter->Init(mSchema, 5, GetParam());
        string docString = "cmd=update_field,string1=hello,price=5,ts=6;";
        AddOperations(opWriter, docString, 5, 6);
        docString = "cmd=update_field,string1=hello,price=5,ts=4;";
        AddOperations(opWriter, docString, 1, 4);
        InMemSegmentOperationIteratorPtr inMemSegOpIter = opWriter->CreateSegmentOperationIterator(0, 0, 5);
        ASSERT_TRUE(inMemSegOpIter);
        docString = "cmd=update_field,string1=hello,price=5,ts=6;";
        AddOperations(opWriter, docString, 3, 6);
        size_t opCount(0);
        while (inMemSegOpIter->HasNext()) {
            OperationBase* op = inMemSegOpIter->Next();
            ASSERT_TRUE(op);
            ++opCount;
        }

        ASSERT_EQ(size_t(5), opCount);
        auto opCursor = inMemSegOpIter->GetLastCursor();
        ASSERT_EQ(5, opCursor.pos);
    }
}

TEST_P(OperationWriterTest, TestCreateSegOpIterator)
{
    OperationWriterPtr opWriter(new OperationWriter());
    opWriter->Init(mSchema, index::DEFAULT_MAX_OPERATION_BLOCK_SIZE, GetParam());
    string docString = "cmd=update_field,string1=hello,price=5,ts=6;";

    // first add 10 operations
    AddOperations(opWriter, docString, 10, 1);

    // test offset out of range
    ASSERT_FALSE(opWriter->CreateSegmentOperationIterator(0, 11, 0));

    // create iterator after 10 ops
    InMemSegmentOperationIteratorPtr inMemSegOpIter = opWriter->CreateSegmentOperationIterator(0, 0, 0);
    ASSERT_TRUE(inMemSegOpIter);

    // continue to add 10 ops
    AddOperations(opWriter, docString, 10, 11);

    // iterate throught the iterator,
    // and expect 10 ops (the number of operations when the iterator was created)
    size_t opCount(0);
    while (inMemSegOpIter->HasNext()) {
        OperationBase* op = inMemSegOpIter->Next();
        ASSERT_TRUE(op);
        ++opCount;
    }

    ASSERT_EQ(size_t(10), opCount);
}

TEST_P(OperationWriterTest, TestCreateSegOpIteratorMultiThread)
{
    OperationWriterPtr opWriter(new OperationWriter());
    opWriter->Init(mSchema, index::DEFAULT_MAX_OPERATION_BLOCK_SIZE, GetParam());

    size_t expectOpCount = 50000;
    int64_t startTimestamp = 1;

    string docString = "cmd=update_field,string1=hello,price=5,ts=6;";

    // launch two threads. one to build operation, the other to read operations
    autil::ThreadPtr addOpThread;
    autil::ThreadPtr readOpThread;
    addOpThread = autil::Thread::createThread(std::bind(&OperationWriterTest::AddOperationThreadFunc, this, opWriter,
                                                        docString, expectOpCount, startTimestamp));

    readOpThread = autil::Thread::createThread(
        std::bind(&OperationWriterTest::ReadOperationThreadFunc, this, opWriter, expectOpCount, startTimestamp));

    addOpThread->join();
    readOpThread->join();
}

void OperationWriterTest::AddOperationThreadFunc(const OperationWriterPtr& opWriter, const string& docString,
                                                 size_t opCount, int64_t startTs)
{
    assert(opWriter);
    AddOperations(opWriter, docString, opCount, startTs);
}

void OperationWriterTest::ReadOperationThreadFunc(const OperationWriterPtr& opWriter, size_t expectOpCount,
                                                  int64_t startTs)
{
    assert(opWriter);
    size_t beginOffset = 0;
    int64_t expectTimestamp = startTs;
    size_t createIterCount = 0;
    InMemSegmentOperationIteratorPtr inMemSegOpIter;
    do {
        inMemSegOpIter = InMemSegmentOperationIteratorPtr();
        while (!inMemSegOpIter && beginOffset < expectOpCount) {
            inMemSegOpIter = opWriter->CreateSegmentOperationIterator(0, beginOffset, 0);
        }

        if (!inMemSegOpIter) {
            break;
        }
        IE_LOG(INFO, "Create Segment Operation Iterator for the %lu-th time", ++createIterCount);

        while (inMemSegOpIter->HasNext()) {
            OperationBase* op = inMemSegOpIter->Next();
            ASSERT_TRUE(op);
            ASSERT_EQ(expectTimestamp++, op->GetTimestamp());
        }
        OperationCursor cur = inMemSegOpIter->GetLastCursor();
        beginOffset = cur.pos + 1;
    } while (beginOffset < expectOpCount);
    ASSERT_EQ(expectOpCount, beginOffset);
}
}} // namespace indexlib::partition
