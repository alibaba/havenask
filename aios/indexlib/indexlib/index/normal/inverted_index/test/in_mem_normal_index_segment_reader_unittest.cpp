#include "indexlib/index/normal/inverted_index/test/in_mem_normal_index_segment_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/document/index_document/normal_document/field.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"

using testing::Return;
using testing::_;
using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemNormalIndexSegmentReaderTest);

InMemNormalIndexSegmentReaderTest::InMemNormalIndexSegmentReaderTest()
{
}

InMemNormalIndexSegmentReaderTest::~InMemNormalIndexSegmentReaderTest()
{
}

void InMemNormalIndexSegmentReaderTest::SetUp()
{
}

void InMemNormalIndexSegmentReaderTest::TearDown()
{
}

void InMemNormalIndexSegmentReaderTest::TestSimpleProcess()
{
    // load index partition config
    IndexPartitionSchemaPtr partitionSchema = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH, "default_index_engine_example.json");
    INDEXLIB_TEST_TRUE(partitionSchema);

    // create normal index reader
    NormalIndexWriter indexWriter(100, IndexPartitionOptions());
    config::IndexSchemaPtr indexSchema = partitionSchema->GetIndexSchema();
    INDEXLIB_TEST_TRUE(indexSchema);
    mIndexConfig = indexSchema->GetIndexConfig("user_name");
    INDEXLIB_TEST_TRUE(mIndexConfig);
    indexWriter.Init(mIndexConfig, NULL);

    // add field: user_name:allen
    mFieldSchema = partitionSchema->GetFieldSchema();
    INDEXLIB_TEST_TRUE(mFieldSchema);
    Field *field = DocumentMaker::MakeField(mFieldSchema, "[user_name: (allen)]");
    indexWriter.AddField(field);
    unique_ptr<Field> filePtr(field);

    PostingFormatOption option;
    SegmentPosting segPosting1(option);
    segPosting1.SetBaseDocId(0);
    segPosting1.SetDocCount(100);

    SegmentPosting segPosting2(option);
    segPosting2.SetBaseDocId(100);
    segPosting2.SetDocCount(50);

    SegmentPostingVector segPostingVec;
    segPostingVec.push_back(segPosting1);
    segPostingVec.push_back(segPosting2);

    DefaultHasher hasher;
    dictkey_t termHashKey;
    hasher.GetHashKey("allen", termHashKey);
    autil::mem_pool::Pool sessionPool(1024);

    SegmentPosting resultSegPosting(option);

    //TODO: only support one reader, rename?
    IndexSegmentReaderPtr indexSegReaderPtr = indexWriter.CreateInMemReader();
    bool ret = indexSegReaderPtr->GetSegmentPosting(termHashKey, 150, 
            resultSegPosting, &sessionPool);

    INDEXLIB_TEST_TRUE(ret);
    INDEXLIB_TEST_EQUAL((docid_t)150, resultSegPosting.GetBaseDocId());

    //TODO: doc count 
    INDEXLIB_TEST_EQUAL((uint32_t)0, resultSegPosting.GetDocCount());
}

IE_NAMESPACE_END(index);

