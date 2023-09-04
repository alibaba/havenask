#include "indexlib/index/kkv/test/kkv_define_unittest.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KkvDefineTest);

KkvDefineTest::KkvDefineTest() {}

KkvDefineTest::~KkvDefineTest() {}

void KkvDefineTest::CaseSetUp() {}

void KkvDefineTest::CaseTearDown() {}

struct LegacyPKeyOffset {
    uint64_t chunkOffset   : 40;
    uint64_t inChunkOffset : 24;

    LegacyPKeyOffset(uint64_t value = 0)
    {
        static_assert(sizeof(*this) == sizeof(value), "to guarantee equal size");
        void* addr = &value;
        *this = *((LegacyPKeyOffset*)addr);
    }

    uint64_t ToU64Value() const { return *((uint64_t*)this); }
};

void KkvDefineTest::TestOnDiskPKeyOffsetCompitable()
{
    LegacyPKeyOffset legacyOffset;
    legacyOffset.chunkOffset = 178382135;
    legacyOffset.inChunkOffset = 4095;

    OnDiskPKeyOffset newOffset(legacyOffset.ToU64Value());
    ASSERT_EQ(legacyOffset.chunkOffset, newOffset.chunkOffset);
    ASSERT_EQ(legacyOffset.inChunkOffset, newOffset.inChunkOffset);
    ASSERT_EQ(DEFAULT_REGIONID, newOffset.regionId);

    newOffset.inChunkOffset = 1033;
    {
        newOffset.regionId = DEFAULT_REGIONID;
        LegacyPKeyOffset newLegacyOffset(newOffset.ToU64Value());
        ASSERT_EQ(newLegacyOffset.chunkOffset, newOffset.chunkOffset);
        ASSERT_EQ(newLegacyOffset.inChunkOffset, newOffset.inChunkOffset);
    }

    {
        newOffset.regionId = 1;
        LegacyPKeyOffset newLegacyOffset(newOffset.ToU64Value());
        ASSERT_EQ(newLegacyOffset.chunkOffset, newOffset.chunkOffset);
        ASSERT_NE(newLegacyOffset.inChunkOffset, newOffset.inChunkOffset);
    }
}

void KkvDefineTest::TestBlockHint()
{
    using offset = OnDiskPKeyOffset;
    auto test = [](pair<uint64_t, uint64_t> a, uint64_t b, int64_t expectedBlock) {
        offset _a(a.first, a.second);
        _a.SetBlockHint(b);
        EXPECT_EQ(expectedBlock * OnDiskPKeyOffset::HINT_BLOCK_SIZE, _a.GetHintSize())
            << a.first << "," << a.second << "   " << b << " " << expectedBlock;
    };
    test({0, 0}, 124, 1);
    test({0, 0}, 4096, 1);
    test({0, 0}, 4097, 2);
    test({4096, 0}, 4097, 1);
    test({4096, 0}, 8192, 1);
    test({4096, 0}, 8193, 2);

    test({233, 0}, 8193, 3);
    test({233, 0}, 98193, 4);

    test({4090, 200}, 8193, 3);
    test({4290, 0}, 8193, 2);
}
}} // namespace indexlib::index
