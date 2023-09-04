#include "indexlib/index/kv/ReclaimedValueCollector.h"

#include "indexlib/framework/mem_reclaimer/EpochBasedMemReclaimer.h"
#include "unittest/unittest.h"
namespace indexlibv2::index {

class ReclaimedValueCollectorTest : public TESTBASE
{
};

TEST_F(ReclaimedValueCollectorTest, testGeneral)
{
    size_t reclaimFreq = 2ul;
    indexlibv2::framework::EpochBasedMemReclaimer memReclaimer(reclaimFreq, nullptr);
    auto offsetCollector = ReclaimedValueCollector<int64_t>::Create(&memReclaimer);
    ASSERT_NE(nullptr, offsetCollector);

    {
        size_t length = 8;
        int64_t offset = 0;
        offsetCollector->Collect(length, offset);
        offsetCollector->Collect(length, offset + 8);
        {
            int64_t reclaimedOffset = -1l;
            ASSERT_FALSE(offsetCollector->PopLengthEqualTo(length, reclaimedOffset));
        }
        memReclaimer.TryReclaim();
        memReclaimer.TryReclaim();
        {
            int64_t reclaimedOffset = -1l;
            ASSERT_TRUE(offsetCollector->PopLengthEqualTo(length, reclaimedOffset));
            ASSERT_EQ(reclaimedOffset, offset + 8);
        }
        {
            int64_t reclaimedOffset = -1l;
            ASSERT_TRUE(offsetCollector->PopLengthEqualTo(length, reclaimedOffset));
            ASSERT_EQ(reclaimedOffset, offset);
        }
        {
            int64_t reclaimedOffset = -1l;
            ASSERT_FALSE(offsetCollector->PopLengthEqualTo(length, reclaimedOffset));
        }
    }
    {
        size_t length = 4;
        int64_t offset = 16;
        offsetCollector->Collect(length, offset);
        memReclaimer.TryReclaim();
        memReclaimer.TryReclaim();
        {
            int64_t reclaimedOffset = -1l;
            ASSERT_FALSE(offsetCollector->PopLengthEqualTo(8, reclaimedOffset));
        }
        {
            int64_t reclaimedOffset = -1l;
            ASSERT_TRUE(offsetCollector->PopLengthEqualTo(4, reclaimedOffset));
            ASSERT_EQ(reclaimedOffset, offset);
        }
    }
}

} // namespace indexlibv2::index
