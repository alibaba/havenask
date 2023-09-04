#include "matchdoc/VectorStorage.h"

#include "gtest/gtest.h"
#include <memory>
#include <sstream>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"

using namespace std;
using namespace autil;

namespace matchdoc {

class VectorStorageTest : public testing::Test {
public:
    void SetUp() override {
        _pool = std::make_shared<autil::mem_pool::Pool>();
        _storage = std::make_unique<VectorStorage>(_pool, 10);
    }

private:
    void checkChunkPool(const vector<std::shared_ptr<autil::mem_pool::Pool>> &expect) {
        ASSERT_EQ(expect.size(), _storage->_chunks.size());
        for (size_t i = 0; i < expect.size(); i++) {
            ASSERT_EQ(expect[i], _storage->_chunks[i].pool);
        }
    }

protected:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::unique_ptr<VectorStorage> _storage;
};

TEST_F(VectorStorageTest, testSimpleProcess) {
    ASSERT_TRUE(_storage->_chunks.empty());

    _storage->growToSize(0);
    ASSERT_TRUE(_storage->_chunks.empty());

    _storage->growToSize(VectorStorage::CHUNKSIZE);
    ASSERT_EQ(1u, _storage->_chunks.size());

    _storage->growToSize(VectorStorage::CHUNKSIZE);
    ASSERT_EQ(1u, _storage->_chunks.size());

    _storage->growToSize(VectorStorage::CHUNKSIZE * 3);
    ASSERT_EQ(3u, _storage->_chunks.size());

    _storage->growToSize(VectorStorage::CHUNKSIZE * 3);
    ASSERT_EQ(3u, _storage->_chunks.size());

    checkChunkPool({_pool, _pool, _pool});
}

TEST_F(VectorStorageTest, testMount) {
    vector<int32_t> docIds;
    int32_t docCount = 5000;
    for (int32_t i = 0; i < docCount; i++) {
        docIds.push_back(i);
    }
    _storage = VectorStorage::fromBuffer(docIds.data(), docIds.size(), _pool);
    for (int32_t i = 0; i < docCount; i++) {
        ASSERT_EQ(i, *(int32_t *)_storage->get(i));
    }
    ASSERT_EQ(5u, _storage->_chunks.size());
    checkChunkPool({nullptr, nullptr, nullptr, nullptr, _pool});
    _storage->growToSize(VectorStorage::CHUNKSIZE * 5);
    ASSERT_EQ(5u, _storage->_chunks.size());
    _storage->growToSize(VectorStorage::CHUNKSIZE * 10);
    ASSERT_EQ(10u, _storage->_chunks.size());
    for (int32_t i = 0; i < docCount; i++) {
        ASSERT_EQ(i, *(int32_t *)_storage->get(i));
    }
}

TEST_F(VectorStorageTest, testAppend) {
    vector<int32_t> docIds;
    vector<int32_t> docIds2;
    int32_t docCount = 5000;
    int32_t capacity = 5 * 1024;
    for (int32_t i = 0; i < docCount; i++) {
        docIds.push_back(i);
        docIds2.push_back(10000 + i);
    }
    {
        auto storage1 = VectorStorage::fromBuffer(docIds.data(), docIds.size(), _pool);
        VectorStorage storage2(_pool, sizeof(int32_t));
        storage2.append(*storage1);
        for (int32_t i = 0; i < docCount; i++) {
            ASSERT_EQ(i, *(int32_t *)storage1->get(i));
            ASSERT_EQ(i, *(int32_t *)storage2.get(i));
        }
    }
    {
        auto storage1 = VectorStorage::fromBuffer(docIds.data(), docIds.size(), _pool);
        auto storage2 = VectorStorage::fromBuffer(docIds2.data(), docIds2.size(), _pool);
        storage1->append(*storage2);
        for (int32_t i = 0; i < docCount; i++) {
            ASSERT_EQ(i, *(int32_t *)storage1->get(i));
            ASSERT_EQ(i + 10000, *(int32_t *)storage2->get(i));
        }
        for (int32_t i = capacity; i < capacity + docCount; i++) {
            ASSERT_EQ(i - capacity + 10000, *(int32_t *)storage1->get(i));
        }
    }
}

TEST_F(VectorStorageTest, testChunkPool) {
    _storage->growToSize(VectorStorage::CHUNKSIZE * 2);
    ASSERT_EQ(2u, _storage->_chunks.size());

    auto pool1 = std::make_shared<autil::mem_pool::Pool>();
    auto pool2 = std::make_shared<autil::mem_pool::Pool>();
    {
        VectorStorage other(pool1, 10);
        other.growToSize(VectorStorage::CHUNKSIZE);
        ASSERT_EQ(1u, other._chunks.size());
        _storage->append(other);
        checkChunkPool({_pool, _pool, pool1});
    }
    {
        VectorStorage other(pool2, 10);
        other.growToSize(VectorStorage::CHUNKSIZE + 1);
        ASSERT_EQ(2u, other._chunks.size());
        _storage->append(other);
        checkChunkPool({_pool, _pool, pool1, pool2, pool2});
    }
    {
        _storage->truncate(VectorStorage::CHUNKSIZE * 3);
        checkChunkPool({_pool, _pool, pool1});
    }
    {
        _storage->truncate(VectorStorage::CHUNKSIZE);
        checkChunkPool({_pool});
    }
}

} // namespace matchdoc
