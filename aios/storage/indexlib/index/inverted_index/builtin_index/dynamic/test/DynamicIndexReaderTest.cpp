#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicIndexReader.h"

#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "indexlib/util/PoolUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DynamicIndexReaderTest : public TESTBASE
{
public:
    DynamicIndexReaderTest() = default;
    ~DynamicIndexReaderTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void DynamicIndexReaderTest::setUp() {}

void DynamicIndexReaderTest::tearDown() {}

TEST_F(DynamicIndexReaderTest, testUsage)
{
    std::string field = "long1:uint32;";
    std::string index = "index1:number:long1";
    std::string attr = "";
    std::string summary = "";
    auto schema = indexlibv2::table::NormalTabletSchemaMaker::Make(field, index, attr, summary);
    auto indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(
        schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, "index1"));

    autil::mem_pool::Pool pool;
    DynamicMemIndexer indexer(100, true);
    indexer.Init(indexConfig);

    std::vector<dictkey_t> keys {100, 200};
    for (auto key : keys) {
        DictKeyInfo keyInfo(key);
        indexer.UpdateTerm(9, keyInfo, document::ModifiedTokens::Operation::ADD);
    }

    indexer.UpdateTerm(9, DictKeyInfo(100), document::ModifiedTokens::Operation::REMOVE);
    indexer.UpdateTerm(9, DictKeyInfo(300), document::ModifiedTokens::Operation::ADD);

    auto inMemReader = indexer.CreateInMemReader();

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

} // namespace indexlib::index
