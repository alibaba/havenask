#include "suez/table/wal/QueueWAL.h"

#include <unittest/unittest.h>

#include "suez/table/wal/WALConfig.h"

using namespace std;
using namespace testing;

namespace suez {

class QueueWALTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void QueueWALTest::setUp() {}

void QueueWALTest::tearDown() {}

TEST_F(QueueWALTest, testInit) {
    {
        WALConfig config;
        QueueWAL wal;
        EXPECT_TRUE(!wal.init(config));
    }
    {
        WALConfig config;
        config.sinkDescription[QueueWAL::QUEUE_NAME] = "test";
        QueueWAL wal;
        EXPECT_TRUE(wal.init(config));
        EXPECT_TRUE(nullptr != wal._docQueuePtr);
        EXPECT_EQ((uint32_t)1000, wal._maxQueueSize);
    }
    {
        WALConfig config;
        config.sinkDescription[QueueWAL::QUEUE_NAME] = "test";
        config.sinkDescription[QueueWAL::QUEUE_SIZE] = "abc";
        QueueWAL wal;
        EXPECT_TRUE(wal.init(config));
        EXPECT_TRUE(nullptr != wal._docQueuePtr);
        EXPECT_EQ((uint32_t)1000, wal._maxQueueSize);
    }
    {
        WALConfig config;
        config.sinkDescription[QueueWAL::QUEUE_NAME] = "test";
        config.sinkDescription[QueueWAL::QUEUE_SIZE] = "2000";
        QueueWAL wal;
        EXPECT_TRUE(wal.init(config));
        EXPECT_TRUE(nullptr != wal._docQueuePtr);
        EXPECT_EQ((uint32_t)2000, wal._maxQueueSize);
    }
}

TEST_F(QueueWALTest, testLogFailed) {
    vector<pair<uint16_t, string>> strs;
    strs.push_back(make_pair<uint16_t, string>(1, "abc"));
    strs.push_back(make_pair<uint16_t, string>(2, "bcd"));
    bool ret = false;
    auto done = [ret](autil::Result<vector<int64_t>> result) { EXPECT_EQ(ret, result.is_ok()); };
    QueueWAL wal;
    wal.log(strs, done);

    WALConfig config;
    config.sinkDescription[QueueWAL::QUEUE_NAME] = "test";
    config.sinkDescription[QueueWAL::QUEUE_SIZE] = "1";
    EXPECT_TRUE(wal.init(config));
    wal.log(strs, done);
}

TEST_F(QueueWALTest, testLogSuccess) {
    vector<pair<uint16_t, string>> strs;
    strs.push_back(make_pair<uint16_t, string>(1, "abc"));
    strs.push_back(make_pair<uint16_t, string>(2, "bcd"));
    bool ret = true;
    auto done = [ret](autil::Result<vector<int64_t>> result) { EXPECT_EQ(ret, result.is_ok()); };

    WALConfig config;
    config.sinkDescription[QueueWAL::QUEUE_NAME] = "test";
    QueueWAL wal;
    EXPECT_TRUE(wal.init(config));
    wal.log(strs, done);
    EXPECT_EQ((uint32_t)2, wal._docQueuePtr->Size());
}

} // namespace suez
