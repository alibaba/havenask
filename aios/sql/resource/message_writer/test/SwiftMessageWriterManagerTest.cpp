#include "sql/resource/SwiftMessageWriterManager.h"

#include <iosfwd>
#include <string>

#include "autil/Log.h"
#include "navi/common.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/fake_client/FakeSwiftClient.h"
#include "swift/client/helper/CheckpointBuffer.h"
#include "swift/common/Common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::common;
using namespace swift::client;
using namespace navi;

namespace sql {

class FakeSwiftMessageWriterManager : public SwiftMessageWriterManager {
private:
    void createSwiftClient() override {
        _swiftClient.reset(new FakeSwiftClient);
    }
};

class SwiftMessageWriterManagerTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, SwiftMessageWriterManagerTest);

void SwiftMessageWriterManagerTest::setUp() {}

void SwiftMessageWriterManagerTest::tearDown() {}

TEST_F(SwiftMessageWriterManagerTest, testInit_parseConfigFailed) {
    navi::NaviLoggerProvider provider("WARN");
    std::string config;
    FakeSwiftMessageWriterManager manager;
    ASSERT_FALSE(manager.init(config));
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "parse swift write config failed [].", provider));
}

TEST_F(SwiftMessageWriterManagerTest, testInit_swiftClientInitFailed) {
    navi::NaviLoggerProvider provider("WARN");
    std::string config = string(R"json(
{
  "table_read_write_config" : {}
}
)json");
    FakeSwiftMessageWriterManager manager;
    ASSERT_FALSE(manager.init(config));
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "init swift client failed", provider));
}

TEST_F(SwiftMessageWriterManagerTest, testInit_swiftWriterInitFailed) {
    navi::NaviLoggerProvider provider("WARN");
    std::string config = string(R"json(
{
  "table_read_write_config" : {
     "table1" : {
     }
  },
  "swift_client_config" : "zkPath=abc"
}
)json");
    FakeSwiftMessageWriterManager manager;
    ASSERT_FALSE(manager.init(config));
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "init swift writer failed", provider));
}

TEST_F(SwiftMessageWriterManagerTest, testInit_sueecss) {
    std::string config = string(R"json(
{
  "table_read_write_config" : {
     "table1" : {
        "swift_topic_name" : "topic1",
        "swift_writer_config" : "{}"
     }
  },
  "swift_client_config" : "zkPath=abc"
}
)json");
    FakeSwiftMessageWriterManager manager;
    ASSERT_TRUE(manager.init(config));
}

} // namespace sql
