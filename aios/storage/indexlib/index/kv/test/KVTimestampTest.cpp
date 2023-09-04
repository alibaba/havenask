#include "indexlib/index/kv/KVTimestamp.h"

#include <limits>

#include "unittest/unittest.h"

namespace indexlibv2::index {

class KVTimestampTest : public TESTBASE
{
};

TEST_F(KVTimestampTest, testNormalize)
{
    int64_t microSeconds = 1024 * 1000 * 1000;
    EXPECT_EQ(1024, KVTimestamp::Normalize(microSeconds));
    EXPECT_EQ(1024, KVTimestamp::Normalize(microSeconds + 1));
    EXPECT_EQ(1024, KVTimestamp::Normalize(microSeconds + 1000));
}

} // namespace indexlibv2::index
