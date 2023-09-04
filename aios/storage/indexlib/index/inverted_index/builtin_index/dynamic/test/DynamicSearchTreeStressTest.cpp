#include <atomic>
#include <map>
#include <random>
#include <thread>

#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicSearchTree.h"
#include "unittest/unittest.h"

namespace indexlib::index {
class DynamicSearchTreeStressTest : public TESTBASE
{
public:
    DynamicSearchTreeStressTest() = default;
    ~DynamicSearchTreeStressTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    autil::mem_pool::Pool _pool;
};

namespace {

struct Context {
    DynamicSearchTree* tree {nullptr};
    std::map<docid_t, bool> data;
    std::atomic<bool> running {false};
    std::atomic<size_t> sum {0};
    size_t mod {0};
};

void ReadRoutine(Context* ctx)
{
    std::mt19937 mt(/*seed_value*/ 0);
    while (ctx->running.load()) {
        docid_t id = mt() % ctx->mod;
        KeyType key;
        if (ctx->tree->Search(id, &key) and !key.IsDelete()) {
            ctx->sum += key.DocId();
        }
    }
}
void WriteRoutine(Context* ctx)
{
    std::mt19937 mt(/*seed_value*/ 0);
    while (ctx->running.load()) {
        docid_t id = mt() % ctx->mod;
        assert(id >= 0);
        bool isDelete = mt() % 2; // 0:add, 1:delete
        if (isDelete) {
            ctx->tree->Remove(id);
        } else {
            ctx->tree->Insert(id);
        }
        ctx->data[id] = isDelete;
    }
}

} // namespace

TEST_F(DynamicSearchTreeStressTest, TestStress)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(4, &nodeManager);
    Context ctx;
    ctx.tree = &tree;
    ctx.running = true;
    ctx.mod = 81920000;

    std::vector<std::thread> readers;
    for (int i = 0; i < 5; ++i) {
        readers.push_back(std::thread([&]() { ReadRoutine(&ctx); }));
    }
    std::thread writer([&]() { WriteRoutine(&ctx); });
    sleep(20);
    ctx.running = false;
    for (auto& t : readers) {
        t.join();
    }
    writer.join();
    for (const auto& item : ctx.data) {
        KeyType key;
        ASSERT_TRUE(tree.Search(item.first, &key));
        ASSERT_EQ(item.second, key.IsDelete());
    }
}

namespace {
void IteratorRoutine(Context* ctx)
{
    std::mt19937 mt(/*seed_value*/ 0);
    while (ctx->running.load()) {
        docid_t id = mt() % ctx->mod;
        KeyType key;
        auto iter = ctx->tree->CreateIterator();
        auto iter2 = ctx->tree->CreateIterator();
        auto iter3 = ctx->tree->CreateIterator();
        auto iter4 = ctx->tree->CreateIterator();
        docid_t lastDoc = INVALID_DOCID;
        while (iter.Seek(id, &key)) {
            ASSERT_LT(lastDoc, key.DocId());
            id = key.DocId();
        }
        docid_t lastDoc2 = INVALID_DOCID;
        while (iter.Seek(id, &key)) {
            ASSERT_LT(lastDoc2, key.DocId());
            id = key.DocId();
        }

        docid_t lastDoc3 = INVALID_DOCID;
        while (iter.Seek(id, &key)) {
            ASSERT_LT(lastDoc3, key.DocId());
            id = key.DocId();
        }
        docid_t lastDoc4 = INVALID_DOCID;
        while (iter.Seek(id, &key)) {
            ASSERT_LT(lastDoc4, key.DocId());
            id = key.DocId();
        }
    }
}
} // namespace

TEST_F(DynamicSearchTreeStressTest, TestIteratorStress)
{
    NodeManager nodeManager(&_pool);
    DynamicSearchTree tree(4, &nodeManager);
    Context ctx;
    ctx.tree = &tree;
    ctx.running = true;
    ctx.mod = 81920000;

    std::vector<std::thread> readers;
    for (int i = 0; i < 5; ++i) {
        readers.push_back(std::thread([&]() { IteratorRoutine(&ctx); }));
    }
    std::thread writer([&]() { WriteRoutine(&ctx); });
    sleep(20);
    ctx.running = false;
    for (auto& t : readers) {
        t.join();
    }
    writer.join();

    auto iter = tree.CreateIterator();
    docid_t doc = INVALID_DOCID;
    KeyType key;
    std::vector<KeyType> results;
    while (iter.Seek(doc, &key)) {
        results.push_back(key);
        doc = key.DocId();
    }
    size_t idx = 0;
    for (const auto& item : ctx.data) {
        auto key = results[idx++];
        ASSERT_EQ(item.first, key.DocId());
        ASSERT_EQ(item.second, key.IsDelete());
    }
}

} // namespace indexlib::index
