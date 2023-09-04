#include "indexlib/index/kkv/common/ChunkDefine.h"

#include "indexlib/index/kkv/Constant.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class ChunkDefineTest : public TESTBASE
{
public:
    void setUp() override {}
};

TEST_F(ChunkDefineTest, testChunkMeta)
{
    {
        ChunkMeta meta {0};
        ASSERT_EQ(0, meta.length);
        ASSERT_EQ(0, meta._reserved);
    }
    {
        ChunkMeta meta {0, 0};
        ASSERT_EQ(0, meta.length);
        ASSERT_EQ(0, meta._reserved);
    }
    {
        ChunkMeta meta {127, 113};
        ASSERT_EQ(127, meta.length);
        ASSERT_EQ(113, meta._reserved);
    }
    {
        ChunkMeta meta {MAX_CHUNK_DATA_LEN, 113};
        ASSERT_EQ(MAX_CHUNK_DATA_LEN, meta.length);
        ASSERT_EQ(113, meta._reserved);
    }
    {
        uint32_t val = MAX_CHUNK_DATA_LEN + 1 + 333; // 故意让值溢出
        ChunkMeta meta {val, 113};
        ASSERT_EQ(333, meta.length);
        ASSERT_EQ(113, meta._reserved);
    }
}

TEST_F(ChunkDefineTest, testChunkData)
{
    auto chunkData = ChunkData::Invalid();
    ASSERT_EQ(nullptr, chunkData.data);
    ASSERT_EQ(0, chunkData.length);

    chunkData = ChunkData::Of((const char*)1, 1024);
    ASSERT_EQ((const char*)1, chunkData.data);
    ASSERT_EQ(1024, chunkData.length);
}

} // namespace indexlibv2::index
