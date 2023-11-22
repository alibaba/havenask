#include "table/ListDataHolder.h"

#include <vector>

#include "unittest/unittest.h"

namespace table {

class ListDataHolderTest : public TESTBASE {};

TEST_F(ListDataHolderTest, testSimple) {
    auto pool = std::make_shared<autil::mem_pool::Pool>();

    using DataHolder = ListDataHolder<uint32_t>;
    DataHolder holder(pool, 8);
    ASSERT_EQ(nullptr, holder._lastChunk);
    ASSERT_EQ(8, holder._chunkSize);
    ASSERT_EQ(holder._chunkSize, holder._lastChunkPos);
    ASSERT_EQ(0, holder._chunks.size());

    uint32_t data[] = {1, 2, 4, 5, 6, 7, 8, 9, 10};
    auto address = holder.append(data, 4);
    ASSERT_TRUE(address != nullptr);
    ASSERT_EQ(address, holder._lastChunk->data);
    ASSERT_EQ(4, holder._lastChunkPos);
    ASSERT_EQ(1, holder._chunks.size());

    address = holder.append(data + 4, 4);
    ASSERT_EQ(address, holder._lastChunk->data + 4);
    ASSERT_EQ(holder._lastChunk->chunkSize, holder._lastChunkPos);
    ASSERT_EQ(1, holder._chunks.size());

    address = holder.append(data, 6);
    ASSERT_EQ(address, holder._lastChunk->data);
    ASSERT_EQ(2, holder._chunks.size());
    ASSERT_EQ(6, holder._lastChunkPos);

    address = holder.append(data, 9);
    ASSERT_EQ(3, holder._chunks.size());
    ASSERT_EQ(address, holder._chunks[2].data);
    ASSERT_EQ(holder._lastChunk->data, holder._chunks[2].data);
    ASSERT_EQ(9, holder._lastChunkPos);

    address = holder.append(data, 1);
    ASSERT_EQ(4, holder._chunks.size());
    ASSERT_EQ(address, holder._lastChunk->data);
    ASSERT_EQ(1, holder._lastChunkPos);
}

} // namespace table
