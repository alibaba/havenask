#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/document/index_document/normal_document/modified_tokens.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingIterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_writer.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/unittest.h"

using namespace indexlib::test;
using namespace indexlib::util;

namespace indexlib { namespace index { namespace legacy {

class DynamicIndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}

    void TestSimple();
    // void TestInMemReader();
};

void DynamicIndexReaderTest::TestSimple()
{
    std::string field = "long1:uint32;";
    std::string index = "index1:number:long1";
    std::string attr = "";
    std::string summary = "";
    auto schema = test::SchemaMaker::MakeSchema(field, index, attr, summary);
    auto indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");

    autil::mem_pool::Pool pool;
    config::IndexPartitionOptions options;
    options.SetIsOnline(true);
    DynamicIndexWriter writer(0, 100, true, options);
    writer.Init(indexConfig, nullptr);
    document::IndexDocument indexDoc(&pool);

    std::vector<dictkey_t> keys {100, 200};
    for (auto key : keys) {
        DictKeyInfo keyInfo(key);
        writer.UpdateTerm(9, keyInfo, document::ModifiedTokens::Operation::ADD);
    }

    writer.UpdateTerm(9, DictKeyInfo(100), document::ModifiedTokens::Operation::REMOVE);
    writer.UpdateTerm(9, DictKeyInfo(300), document::ModifiedTokens::Operation::ADD);

    auto inMemReader = writer.CreateInMemReader();

    DynamicIndexReader reader;
    reader.TEST_SetIndexConfig(indexConfig);
    reader.AddInMemSegmentReader(/*baseDocId=*/1000, inMemReader);
    DictKeyInfo termKey(200);
    DynamicPostingIterator* iter = reader.Lookup(termKey, {}, /*statePoolSize=*/1000, &pool);
    ASSERT_TRUE(iter);
    EXPECT_EQ(1009, iter->SeekDoc(INVALID_DOCID));
    EXPECT_EQ(INVALID_DOCID, iter->SeekDoc(9));
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, iter);
}

INDEXLIB_UNIT_TEST_CASE(DynamicIndexReaderTest, TestSimple);
}}} // namespace indexlib::index::legacy
