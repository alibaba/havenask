#include "indexlib/index/kkv/building/BuildingSKeyIterator.h"

#include "autil/mem_pool/Pool.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
namespace indexlibv2::index {

using SKeyWriterTyped = SKeyWriter<uint64_t>;
using SKeyNode = SKeyWriterTyped::SKeyNode;

class BuildingSKeyIteratorTest : public TESTBASE
{
};

TEST_F(BuildingSKeyIteratorTest, TestGenenal)
{
    int64_t reserveSize = 1024 * sizeof(SKeyNode);
    size_t longTailThreshold = 100;
    auto skeyWriter = std::make_shared<SKeyWriterTyped>();
    skeyWriter->Init(reserveSize, longTailThreshold);

    vector<SKeyNode> skeyNodes;
    SKeyListInfo listInfo = SKeyListInfo(0, index::INVALID_SKEY_OFFSET, 1);

    for (uint64_t i = 0; i < 256; ++i) {
        auto skeyNode = SKeyNode(i * 2, i, i, i, index::INVALID_SKEY_OFFSET);
        uint32_t skeyOffset = skeyWriter->Append(skeyNode);
        skeyNodes.push_back(skeyNode);
        if (i > 0) {
            skeyWriter->LinkSkeyNode(listInfo, skeyOffset);
        }
    }
    ASSERT_NE(index::INVALID_SKEY_OFFSET, listInfo.blockHeader);

    {
        BuildingSKeyIterator<uint64_t> skeyIterator(skeyWriter, listInfo);
        ASSERT_FALSE(skeyIterator.HasPKeyDeleted());
        size_t i = 0;
        for (; i < 256 && skeyIterator.IsValid(); ++i, skeyIterator.MoveToNext()) {
            ASSERT_EQ(skeyNodes[i].skey, skeyIterator.GetSKey());
            ASSERT_EQ(false, skeyIterator.IsDeleted());
            ASSERT_EQ(skeyNodes[i].valueOffset, skeyIterator.GetValueOffset());
            ASSERT_EQ(skeyNodes[i].expireTime, skeyIterator.GetExpireTime());
            ASSERT_EQ(skeyNodes[i].timestamp, skeyIterator.GetTs());
        }
        ASSERT_EQ(256, i);
        ASSERT_FALSE(skeyIterator.IsValid());
    }
    {
        BuildingSKeyIterator<uint64_t> skeyIterator(skeyWriter, listInfo);
        ASSERT_FALSE(skeyIterator.MoveToSKey(3));
        ASSERT_EQ(4, skeyIterator.GetSKey());
        ASSERT_EQ(2, skeyIterator.GetValueOffset());
        ASSERT_EQ(2, skeyIterator.GetExpireTime());
        ASSERT_EQ(2, skeyIterator.GetTs());

        ASSERT_TRUE(skeyIterator.MoveToSKey(8));
        ASSERT_EQ(8, skeyIterator.GetSKey());
        ASSERT_EQ(4, skeyIterator.GetValueOffset());
        ASSERT_EQ(4, skeyIterator.GetExpireTime());
        ASSERT_EQ(4, skeyIterator.GetTs());

        ASSERT_FALSE(skeyIterator.MoveToSKey(256 * 2));
        ASSERT_FALSE(skeyIterator.IsValid());
    }
}

TEST_F(BuildingSKeyIteratorTest, TestNoSkipList)
{
    int64_t reserveSize = 1024 * sizeof(SKeyNode);
    size_t longTailThreshold = 100;
    auto skeyWriter = std::make_shared<SKeyWriterTyped>();
    skeyWriter->Init(reserveSize, longTailThreshold);

    vector<SKeyNode> skeyNodes;
    SKeyListInfo listInfo = SKeyListInfo(0, index::INVALID_SKEY_OFFSET, 1);

    for (uint64_t i = 0; i < 99; ++i) {
        auto skeyNode = SKeyNode(i * 2, i, i, i, index::INVALID_SKEY_OFFSET);
        uint32_t skeyOffset = skeyWriter->Append(skeyNode);
        skeyNodes.push_back(skeyNode);
        if (i > 0) {
            skeyWriter->LinkSkeyNode(listInfo, skeyOffset);
        }
    }
    ASSERT_EQ(index::INVALID_SKEY_OFFSET, listInfo.blockHeader);

    BuildingSKeyIterator<uint64_t> skeyIterator(skeyWriter, listInfo);
    ASSERT_FALSE(skeyIterator.MoveToSKey(3));
    ASSERT_EQ(4, skeyIterator.GetSKey());
    ASSERT_EQ(2, skeyIterator.GetValueOffset());
    ASSERT_EQ(2, skeyIterator.GetExpireTime());
    ASSERT_EQ(2, skeyIterator.GetTs());

    ASSERT_TRUE(skeyIterator.MoveToSKey(8));
    ASSERT_EQ(8, skeyIterator.GetSKey());
    ASSERT_EQ(4, skeyIterator.GetValueOffset());
    ASSERT_EQ(4, skeyIterator.GetExpireTime());
    ASSERT_EQ(4, skeyIterator.GetTs());

    ASSERT_FALSE(skeyIterator.MoveToSKey(256 * 2));
    ASSERT_FALSE(skeyIterator.IsValid());
}

TEST_F(BuildingSKeyIteratorTest, TestHasPKeyDeleted)
{
    int64_t reserveSize = 1024 * sizeof(SKeyNode);
    size_t longTailThreshold = 100;
    auto skeyWriter = std::make_shared<SKeyWriterTyped>();
    skeyWriter->Init(reserveSize, longTailThreshold);
    uint32_t pkeyDeletedTs = 1024;
    uint32_t skeyOffset =
        skeyWriter->Append(SKeyNode(std::numeric_limits<uint64_t>::min(), SKEY_ALL_DELETED_OFFSET, pkeyDeletedTs,
                                    indexlib::UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET));

    vector<SKeyNode> skeyNodes;
    SKeyListInfo listInfo = SKeyListInfo(skeyOffset, index::INVALID_SKEY_OFFSET, 1);
    for (uint64_t i = 0; i < 99; ++i) {
        auto skeyNode = SKeyNode(i * 2, i, i, i, index::INVALID_SKEY_OFFSET);
        uint32_t skeyOffset = skeyWriter->Append(skeyNode);
        skeyNodes.push_back(skeyNode);
        skeyWriter->LinkSkeyNode(listInfo, skeyOffset);
    }
    {
        BuildingSKeyIterator<uint64_t> skeyIterator(skeyWriter, listInfo);
        ASSERT_TRUE(skeyIterator.HasPKeyDeleted());
        ASSERT_EQ(pkeyDeletedTs, skeyIterator.GetPKeyDeletedTs());
        size_t i = 0;
        for (; i < 99 && skeyIterator.IsValid(); ++i, skeyIterator.MoveToNext()) {
            ASSERT_EQ(skeyNodes[i].skey, skeyIterator.GetSKey());
            ASSERT_EQ(false, skeyIterator.IsDeleted());
            ASSERT_EQ(skeyNodes[i].valueOffset, skeyIterator.GetValueOffset());
            ASSERT_EQ(skeyNodes[i].expireTime, skeyIterator.GetExpireTime());
            ASSERT_EQ(skeyNodes[i].timestamp, skeyIterator.GetTs());
        }
        ASSERT_EQ(99, i);
        ASSERT_FALSE(skeyIterator.IsValid());
    }
}

} // namespace indexlibv2::index
