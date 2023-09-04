#include "suez/table/wal/WALStrategy.h"

#include <unittest/unittest.h>

#include "suez/table/wal/WALConfig.h"

using namespace std;
using namespace testing;

namespace suez {

class WALStrategyTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void WALStrategyTest::setUp() {}

void WALStrategyTest::tearDown() {}

TEST_F(WALStrategyTest, testCreate) {
    {
        WALConfig config;
        auto strategy = WALStrategy::create(config, nullptr);
        EXPECT_TRUE(nullptr != strategy);
    }
    {
        WALConfig config;
        config.strategy = "sync";
        auto strategy = WALStrategy::create(config, nullptr);
        EXPECT_TRUE(nullptr != strategy);
    }
    {
        WALConfig config;
        config.strategy = "queue";
        config.sinkDescription["queue_name"] = "table1";
        auto strategy = WALStrategy::create(config, nullptr);
        EXPECT_TRUE(nullptr != strategy);
    }
    {
        WALConfig config;
        config.strategy = "queue";
        auto strategy = WALStrategy::create(config, nullptr);
        EXPECT_TRUE(nullptr == strategy);
    }
}

} // namespace suez
