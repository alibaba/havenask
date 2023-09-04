#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/document/index_document/normal_document/modified_tokens.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_writer.h"
#include "indexlib/index/test/index_document_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/unittest.h"

using namespace indexlib::test;
using namespace indexlib::util;

namespace indexlib { namespace index {

class DynamicIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}

    void TestSimple();
};

void DynamicIndexWriterTest::TestSimple()
{
    autil::mem_pool::Pool pool;
    config::IndexPartitionOptions options;
    options.SetIsOnline(true);
    DynamicIndexWriter writer(0, 100, true, options);
    config::SingleFieldIndexConfigPtr indexConfig;
    IndexDocumentMaker::CreateSingleFieldIndexConfig(indexConfig, "test", it_number, ft_int32, 0);
    writer.Init(indexConfig, nullptr);
    document::IndexDocument indexDoc(&pool);

    std::vector<dictkey_t> keys {100, 200};
    writer.UpdateTerm(9, DictKeyInfo(100), document::ModifiedTokens::Operation::ADD);
    writer.UpdateTerm(9, DictKeyInfo(200), document::ModifiedTokens::Operation::ADD);

    writer.UpdateTerm(9, DictKeyInfo(100), document::ModifiedTokens::Operation::REMOVE);
    writer.UpdateTerm(9, DictKeyInfo(300), document::ModifiedTokens::Operation::ADD);

    {
        auto tree = writer.TEST_GetPostingWriter(100)->TEST_GetPostingTree();
        ASSERT_TRUE(tree);
        KeyType result;
        ASSERT_TRUE(tree->Search(9, &result));
        ASSERT_EQ(9, result.DocId());
        ASSERT_TRUE(result.IsDelete());
    }
    {
        auto tree = writer.TEST_GetPostingWriter(200)->TEST_GetPostingTree();
        ASSERT_TRUE(tree);
        KeyType result;
        ASSERT_TRUE(tree->Search(9, &result));
        ASSERT_EQ(9, result.DocId());
        ASSERT_FALSE(result.IsDelete());
    }
    {
        auto tree = writer.TEST_GetPostingWriter(300)->TEST_GetPostingTree();
        ASSERT_TRUE(tree);
        KeyType result;
        ASSERT_TRUE(tree->Search(9, &result));
        ASSERT_EQ(9, result.DocId());
        ASSERT_FALSE(result.IsDelete());
    }
}

INDEXLIB_UNIT_TEST_CASE(DynamicIndexWriterTest, TestSimple);
}} // namespace indexlib::index
