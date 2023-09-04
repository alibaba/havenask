#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicSearchTree.h"

#include <random>

#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "unittest/unittest.h"

namespace indexlib::index {
class DynamicSearchTreeTest : public TESTBASE
{
public:
    DynamicSearchTreeTest() = default;
    ~DynamicSearchTreeTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    void StandardTestWithParam(size_t maxSlotShift, size_t numDoc);

private:
    autil::mem_pool::Pool _pool;
};

TEST_F(DynamicSearchTreeTest, TestSimple)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(4, &nodeManager);
    KeyType found;

    EXPECT_FALSE(tree.Search(0, &found));
    tree.Insert(100);
    EXPECT_TRUE(tree.Search(0, &found));
    EXPECT_EQ(100, found.DocId());

    tree.Insert(10);
    tree.Insert(200);

    EXPECT_TRUE(tree.Search(0, &found));
    EXPECT_EQ(10, found.DocId());
    EXPECT_TRUE(tree.Search(15, &found));
    EXPECT_EQ(100, found.DocId());
    EXPECT_TRUE(tree.Search(115, &found));
    EXPECT_EQ(200, found.DocId());
    EXPECT_FALSE(tree.Search(215, &found));
}

TEST_F(DynamicSearchTreeTest, TestSequenceInsert)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(2, &nodeManager);
    std::vector<int32_t> ids;

    for (size_t i = 0; i < 100; ++i) {
        ids.push_back(i);
        tree.Insert(ids.back());
    }

    KeyType found;
    for (int32_t id : ids) {
        EXPECT_TRUE(tree.Search(id, &found));
        EXPECT_EQ(id, found.DocId());
    }
}

TEST_F(DynamicSearchTreeTest, TestReverseInsert)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(4, &nodeManager);
    std::vector<int32_t> ids;

    for (size_t i = 0; i < 100; ++i) {
        ids.push_back(99 - i);
        tree.Insert(ids.back());
    }
    KeyType found;
    for (int32_t id : ids) {
        EXPECT_TRUE(tree.Search(id, &found));
        EXPECT_EQ(id, found.DocId());
    }
}

TEST_F(DynamicSearchTreeTest, TestInsertDuplicate)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(4, &nodeManager);
    std::vector<int32_t> ids;

    for (size_t i = 0; i < 100; ++i) {
        ids.push_back(i);
        tree.Insert(ids.back());
    }
    KeyType found;
    for (size_t i = 0; i < 100; ++i) {
        tree.Insert(99 - i);
        EXPECT_TRUE(tree.Search(i, &found));
        EXPECT_EQ(i, found.DocId());
    }
}

TEST_F(DynamicSearchTreeTest, TestRandomInsert)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(4, &nodeManager);
    std::vector<uint32_t> ids;

    std::mt19937 mt(/*seed_value=*/0);
    for (size_t i = 0; i < 100; ++i) {
        uint32_t id = mt() & 0x7fffffff;
        ids.push_back(id);
        tree.Insert(ids.back());
    }
    KeyType found;
    for (uint32_t id : ids) {
        EXPECT_TRUE(tree.Search(id, &found));
        EXPECT_EQ(id, found.DocId());
    }
}

// Test that seek and remove won't crash for empty tree.
TEST_F(DynamicSearchTreeTest, TestEmptyTree)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(4, &nodeManager);
    KeyType found;
    EXPECT_FALSE(tree.Search(0, &found));
    EXPECT_FALSE(tree.Search(100, &found));
    EXPECT_NO_THROW(tree.Remove(1));
}

void DynamicSearchTreeTest::StandardTestWithParam(size_t maxSlotShift, size_t numDoc)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(maxSlotShift, &nodeManager);
    std::vector<uint32_t> ids;

    std::mt19937 mt(/*seed_value=*/0);
    for (size_t i = 0; i < numDoc; ++i) {
        uint32_t id = (mt() & 0x7fffffff) % 8192;
        // uint32_t id = i;
        ids.push_back(id);
        tree.Insert(ids.back());
    }
    KeyType found;
    for (uint32_t id : ids) {
        EXPECT_TRUE(tree.Search(id, &found));
        EXPECT_EQ(id, found.DocId());
    }
    for (uint32_t id : ids) {
        tree.Remove(id);
        EXPECT_TRUE(tree.Search(id, &found));
    }
    EXPECT_TRUE(tree.Search(0, &found));
}

TEST_F(DynamicSearchTreeTest, TestSmallTree)
{
    StandardTestWithParam(/*maxSlotShift=*/1, /*numDoc=*/100);
    StandardTestWithParam(/*maxSlotShift=*/1, /*numDoc=*/1000);
}
TEST_F(DynamicSearchTreeTest, TestDeepTree)
{
    // Test tree with multiple levels, 30000 nodes will have 5 levels.
    StandardTestWithParam(/*maxSlotShift=*/3, /*numDoc=*/30000);
    StandardTestWithParam(/*maxSlotShift=*/3, /*numDoc=*/5000);
    StandardTestWithParam(/*maxSlotShift=*/2, /*numDoc=*/100);
}

TEST_F(DynamicSearchTreeTest, TestSequenceDelete)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(4, &nodeManager);
    std::vector<uint32_t> ids;

    for (size_t i = 0; i < 100; ++i) {
        ids.push_back(i);
        tree.Insert(ids.back());
    }

    KeyType found;
    for (size_t i = 0; i < 100; ++i) {
        EXPECT_TRUE(tree.Search(i, &found));
        EXPECT_EQ(i, found.DocId());
        EXPECT_FALSE(found.IsDelete());
        tree.Remove(i);
        EXPECT_TRUE(tree.Search(i, &found));
        EXPECT_EQ(i, found.DocId());
        EXPECT_TRUE(found.IsDelete());
    }
    EXPECT_TRUE(tree.Search(0, &found));
}

TEST_F(DynamicSearchTreeTest, TestDeleteSimple)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(4, &nodeManager);

    tree.Insert(10);
    tree.Insert(5);
    tree.Insert(30);

    KeyType found;
    EXPECT_TRUE(tree.Search(30, &found));
    EXPECT_EQ(30, found.DocId());
    EXPECT_FALSE(found.IsDelete());
    tree.Remove(30);
    EXPECT_TRUE(tree.Search(30, &found));
    EXPECT_EQ(30, found.DocId());
    EXPECT_TRUE(found.IsDelete());

    EXPECT_TRUE(tree.Search(5, &found));
    EXPECT_EQ(5, found.DocId());
    EXPECT_FALSE(found.IsDelete());
    tree.Remove(5);
    EXPECT_TRUE(tree.Search(5, &found));
    EXPECT_EQ(5, found.DocId());
    EXPECT_TRUE(found.IsDelete());

    EXPECT_TRUE(tree.Search(10, &found));
    EXPECT_EQ(10, found.DocId());
    EXPECT_FALSE(found.IsDelete());
    tree.Remove(10);
    EXPECT_TRUE(tree.Search(10, &found));
    EXPECT_EQ(10, found.DocId());
    EXPECT_TRUE(found.IsDelete());

    EXPECT_TRUE(tree.Search(0, &found));
}

TEST_F(DynamicSearchTreeTest, TestDeleteThenAdd)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(4, &nodeManager);
    std::vector<int32_t> ids;

    for (size_t i = 0; i < 100; ++i) {
        ids.push_back(i);
        tree.Insert(ids.back());
    }

    for (int32_t id : ids) {
        tree.Remove(id);
    }

    for (int32_t id : ids) {
        tree.Insert(id);
    }
    KeyType found;
    for (int32_t id : ids) {
        EXPECT_TRUE(tree.Search(id, &found));
        EXPECT_EQ(id, found.DocId());
    }
}

TEST_F(DynamicSearchTreeTest, TestIteratorSeek)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(3, &nodeManager);
    std::set<uint32_t> docSet;
    std::mt19937 mt(/*seed_value*/ 0);
    for (size_t i = 0; i < 30000; ++i) {
        uint32_t id = mt() & 0x7fffffff;
        tree.Insert(id);
        docSet.insert(id);
    }
    auto iter = tree.CreateIterator();
    docid_t doc = INVALID_DOCID;
    KeyType key;
    std::vector<docid_t> results;
    while (iter.Seek(doc, &key)) {
        results.push_back(key.DocId());
        doc = key.DocId();
    }
    ASSERT_EQ(docSet.size(), results.size());
    size_t idx = 0;
    for (auto doc : docSet) {
        ASSERT_EQ(doc, results[idx++]);
    }
}

} // namespace indexlib::index
