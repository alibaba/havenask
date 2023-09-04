#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicMemIndexer.h"

#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/inverted_index/config/test/InvertedIndexConfigCreator.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DynamicMemIndexerTest : public TESTBASE
{
public:
    DynamicMemIndexerTest() = default;
    ~DynamicMemIndexerTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void DynamicMemIndexerTest::setUp() {}

void DynamicMemIndexerTest::tearDown() {}

TEST_F(DynamicMemIndexerTest, testUsage)
{
    DynamicMemIndexer indexer(/*hashMapInitSize*/ 100, /*isNumberIndex*/ true);
    auto indexConfig = InvertedIndexConfigCreator::CreateSingleFieldIndexConfig("test", it_number, ft_int32, 0);
    indexer.Init(indexConfig);

    std::vector<dictkey_t> keys {100, 200};
    indexer.UpdateTerm(9, DictKeyInfo(100), document::ModifiedTokens::Operation::ADD);
    indexer.UpdateTerm(9, DictKeyInfo(200), document::ModifiedTokens::Operation::ADD);
    indexer.UpdateTerm(9, DictKeyInfo(100), document::ModifiedTokens::Operation::REMOVE);
    indexer.UpdateTerm(9, DictKeyInfo(300), document::ModifiedTokens::Operation::ADD);

    {
        auto tree = indexer.TEST_GetPostingWriter(100)->TEST_GetPostingTree();
        ASSERT_TRUE(tree);
        KeyType result;
        ASSERT_TRUE(tree->Search(9, &result));
        ASSERT_EQ(9, result.DocId());
        ASSERT_TRUE(result.IsDelete());
    }
    {
        auto tree = indexer.TEST_GetPostingWriter(200)->TEST_GetPostingTree();
        ASSERT_TRUE(tree);
        KeyType result;
        ASSERT_TRUE(tree->Search(9, &result));
        ASSERT_EQ(9, result.DocId());
        ASSERT_FALSE(result.IsDelete());
    }
    {
        auto tree = indexer.TEST_GetPostingWriter(300)->TEST_GetPostingTree();
        ASSERT_TRUE(tree);
        KeyType result;
        ASSERT_TRUE(tree->Search(9, &result));
        ASSERT_EQ(9, result.DocId());
        ASSERT_FALSE(result.IsDelete());
    }
}

} // namespace indexlib::index
