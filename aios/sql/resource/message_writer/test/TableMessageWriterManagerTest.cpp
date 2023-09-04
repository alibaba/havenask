#include "sql/resource/TableMessageWriterManager.h"

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "autil/result/ForwardList.h"
#include "navi/common.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "sql/resource/MessageWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace suez;
using namespace autil;
using namespace autil::result;
using namespace navi;

namespace sql {

class TableMessageWriterManagerTest : public TESTBASE {};

TEST_F(TableMessageWriterManagerTest, testInitFailed) {
    navi::NaviLoggerProvider provider("WARN");
    TableMessageWriterManager manager(nullptr);
    ASSERT_FALSE(manager.init(R"json({"zone_names":""})json"));
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "parse table write config failed", provider));
}

TEST_F(TableMessageWriterManagerTest, testInitSuccess) {
    TableMessageWriterManager manager(nullptr);
    ASSERT_TRUE(manager.init(R"json({"zone_names":["a", "b"]})json"));
}

TEST_F(TableMessageWriterManagerTest, testGetMessageWriter) {
    TableMessageWriterManager manager(nullptr);
    ASSERT_TRUE(manager.init(R"json({"zone_names":["a", "b"]})json"));
    auto w1 = manager.getMessageWriter("a", "t1");
    ASSERT_TRUE(w1);
    ASSERT_EQ(1, manager._tableMessageWriterMap.size());
    ASSERT_EQ(1, manager._tableMessageWriterMap["a"].size());
    auto w2 = manager.getMessageWriter("a", "t1");
    ASSERT_TRUE(w2);
    ASSERT_EQ(w1, w2);
    ASSERT_EQ(1, manager._tableMessageWriterMap.size());
    ASSERT_EQ(1, manager._tableMessageWriterMap["a"].size());
    auto w3 = manager.getMessageWriter("b", "t1");
    ASSERT_TRUE(w3);
    ASSERT_NE(w1, w3);
    ASSERT_EQ(2, manager._tableMessageWriterMap.size());
    ASSERT_EQ(1, manager._tableMessageWriterMap["a"].size());
    ASSERT_EQ(1, manager._tableMessageWriterMap["b"].size());
    auto w4 = manager.getMessageWriter("b", "t2");
    ASSERT_TRUE(w4);
    ASSERT_NE(w3, w4);
    ASSERT_EQ(2, manager._tableMessageWriterMap.size());
    ASSERT_EQ(1, manager._tableMessageWriterMap["a"].size());
    ASSERT_EQ(2, manager._tableMessageWriterMap["b"].size());
    auto w5 = manager.getMessageWriter("c", "t1");
    ASSERT_FALSE(w5);
    ASSERT_EQ(2, manager._tableMessageWriterMap.size());
    ASSERT_EQ(1, manager._tableMessageWriterMap["a"].size());
    ASSERT_EQ(2, manager._tableMessageWriterMap["b"].size());
}

void abenchGetMessageTable(TableMessageWriterManager &manager, const std::string &zoneName) {
    for (size_t i = 1; i < 1000; ++i) {
        string tableName = StringUtil::toString(i % 100);
        if (zoneName == "a" || zoneName == "b") {
            ASSERT_TRUE(manager.getMessageWriter(zoneName, tableName));
        } else {
            ASSERT_FALSE(manager.getMessageWriter(zoneName, tableName));
        }
    }
}

TEST_F(TableMessageWriterManagerTest, testMultiThread) {
    TableMessageWriterManager manager(nullptr);
    ASSERT_TRUE(manager.init(R"json({"zone_names":["a", "b"]})json"));
    std::vector<string> zoneNames = {"a", "b", "c", "d", "a", "b", "a", "b", "a", "b"};
    std::vector<autil::ThreadPtr> threads;
    for (const auto &zoneName : zoneNames) {
        autil::ThreadPtr thread
            = autil::Thread::createThread([&]() { abenchGetMessageTable(manager, zoneName); });
        threads.push_back(thread);
    }
    for (const auto &thread : threads) {
        thread->join();
    }
    ASSERT_EQ(2, manager._tableMessageWriterMap.size());
    ASSERT_EQ(100, manager._tableMessageWriterMap["a"].size());
    ASSERT_EQ(100, manager._tableMessageWriterMap["b"].size());
}

} // namespace sql
