#include "indexlib/index/normal/inverted_index/test/in_mem_normal_index_segment_reader_unittest.h"

#include "indexlib/document/index_document/normal_document/field.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/util/KeyHasherTyped.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

using testing::_;
using testing::Return;
using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InMemNormalIndexSegmentReaderTest);

InMemNormalIndexSegmentReaderTest::InMemNormalIndexSegmentReaderTest() {}

InMemNormalIndexSegmentReaderTest::~InMemNormalIndexSegmentReaderTest() {}

void InMemNormalIndexSegmentReaderTest::TestSimpleProcess()
{
    // load index partition config
    IndexPartitionSchemaPtr partitionSchema =
        SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "default_index_engine_example.json");
    INDEXLIB_TEST_TRUE(partitionSchema);

    // create normal index reader
    NormalIndexWriter indexWriter(INVALID_SEGMENTID, 100, IndexPartitionOptions());
    config::IndexSchemaPtr indexSchema = partitionSchema->GetIndexSchema();
    INDEXLIB_TEST_TRUE(indexSchema);
    mIndexConfig = indexSchema->GetIndexConfig("user_name");
    auto iter = mIndexConfig->CreateIterator();
    while (iter.HasNext()) {
        iter.Next()->SetEnableNullField(true);
    }
    INDEXLIB_TEST_TRUE(mIndexConfig);
    indexWriter.Init(mIndexConfig, NULL);

    // add field: user_name:allen
    mFieldSchema = partitionSchema->GetFieldSchema();
    INDEXLIB_TEST_TRUE(mFieldSchema);
    Field* field = DocumentMaker::MakeField(mFieldSchema, "[user_name: (allen)]");
    unique_ptr<Field> filePtr(field);
    indexWriter.AddField(field);

    dictkey_t termHashKey;
    const string term("allen");
    EXPECT_TRUE(DefaultHasher::GetHashKey(term.data(), term.size(), termHashKey));
    autil::mem_pool::Pool sessionPool(1024);

    PostingFormatOption option;
    SegmentPosting resultSegPosting(option);

    // TODO: only support one reader, rename?
    IndexSegmentReaderPtr indexSegReaderPtr = indexWriter.CreateInMemReader();
    bool ret =
        indexSegReaderPtr->GetSegmentPosting(index::DictKeyInfo(termHashKey), 150, resultSegPosting, &sessionPool);

    INDEXLIB_TEST_TRUE(ret);
    INDEXLIB_TEST_EQUAL((docid_t)150, resultSegPosting.GetBaseDocId());

    // TODO: doc count
    INDEXLIB_TEST_EQUAL((uint32_t)0, resultSegPosting.GetDocCount());
    ret = indexSegReaderPtr->GetSegmentPosting(index::DictKeyInfo::NULL_TERM, 150, resultSegPosting, &sessionPool);
    ASSERT_FALSE(ret);

    NullField nullField;
    nullField.SetFieldId(mFieldSchema->GetFieldConfig("user_name")->GetFieldId());
    indexWriter.AddField(&nullField);
    SegmentPosting segPosting(option);
    ret = indexSegReaderPtr->GetSegmentPosting(index::DictKeyInfo::NULL_TERM, 150, segPosting, &sessionPool);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(segPosting.GetInMemPostingWriter());
}
}} // namespace indexlib::index
