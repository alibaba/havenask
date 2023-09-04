#include "swift/monitor/BrokerMetricsReporter.h"

#include <iostream>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>

#include "autil/TimeUtility.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "unittest/unittest.h"

using namespace std;
using namespace kmonitor;
using namespace autil;
using namespace std;
using namespace testing;

namespace swift {
namespace monitor {

class BrokerMetricsReporterTest : public TESTBASE {
public:
    BrokerMetricsReporterTest() {
        std::string json_string = "{\"tenant_name\":\"tpp\","
                                  "\"service_name\":\"kmon\","
                                  "\"system_metrics\":true,"
                                  "\"sink_period\": 20,"
                                  "\"sink_queue_capacity\": 1000,"
                                  "\"sink_address\":\"localhost:4141\","
                                  "\"global_tags\":"
                                  "{\"cluster\":\"online\",\"app\":\"hub\"}}";
        KMonitorFactory::Init(json_string);
        KMonitorFactory::Start();
    }

    virtual ~BrokerMetricsReporterTest() { KMonitorFactory::Shutdown(); }
    void reportBackupThread(size_t count, int32_t threadNum);
    void reportMutliThread(size_t count, int32_t threadNum);
    void setUp();
    void tearDown();
};

void BrokerMetricsReporterTest::setUp() {}

void BrokerMetricsReporterTest::tearDown() {}

TEST_F(BrokerMetricsReporterTest, testGetAccessInfoMetricsTags) {
    BrokerMetricsReporter reporter;
    AccessInfoCollector collector1("topic1_aaaaaaaa", 0, "1.1", "java", "ea119", "sendMessage");
    map<string, string> tagsMap = {{"clientType", "java"},
                                   {"clientVersion", "1.1"},
                                   {"partition", "0"},
                                   {"topic", "topic1_aaaaaaaa"},
                                   {"type", "sendMessage"},
                                   {"srcDrc", "ea119"}};
    auto tags1 = reporter.getAccessInfoMetricsTags(collector1);
    ASSERT_EQ(1, reporter._accessInfoTagsMap.size());
    auto tags2 = reporter.getAccessInfoMetricsTags(collector1);
    ASSERT_EQ(1, reporter._accessInfoTagsMap.size());
    ASSERT_EQ(tags1.get(), tags2.get());
    ASSERT_EQ(tagsMap, tags1->GetTagsMap());
}

TEST_F(BrokerMetricsReporterTest, testGetPartitionInfoMetricsTags) {
    BrokerMetricsReporter reporter;
    string topicName = "test";
    string partId = "1";
    string accessId = "123456789";
    map<string, string> tagsMap = {{"partition", "1"}, {"topic", "test"}, {"access_id", "123456789"}};
    auto tags1 = reporter.getPartitionInfoMetricsTags(topicName, partId, accessId);
    ASSERT_EQ(1, reporter._partitionInfoTagsMap.size());
    auto tags2 = reporter.getPartitionInfoMetricsTags(topicName, partId, accessId);
    ASSERT_EQ(1, reporter._partitionInfoTagsMap.size());
    ASSERT_EQ(tags1.get(), tags2.get());
    ASSERT_EQ(tagsMap, tags1->GetTagsMap());
}

void BrokerMetricsReporterTest::reportBackupThread(size_t count, int32_t threadNum) {
    int64_t begT = TimeUtility::currentTime();
    AccessInfoCollectorPtr collector(
        new AccessInfoCollector("topic1_aaaaaaaa", 0, "1.1", "java", "ea119", "sendMessage"));
    collector->responseLength = 1000;
    collector->requestLength = 100;
    {
        BrokerMetricsReporter reporter(threadNum);
        for (size_t i = 0; i < count; i++) {
            reporter.reportAccessInfoMetricsBackupThread(collector);
        }
    }
    int64_t endT = TimeUtility::currentTime();
    cout << "report backup thread: " << threadNum << ", count: " << count << ", time: " << (endT - begT) / 1000 << "ms"
         << endl;
}

TEST_F(BrokerMetricsReporterTest, testReportBackupThread) {
    size_t count = 2000000;
    reportBackupThread(count, 0);
    reportBackupThread(count, 1);
    reportBackupThread(count, 2);
    reportBackupThread(count, 50);
    reportBackupThread(count, 100);
}

// void BrokerMetricsReporterTest::reportMutliThread(size_t count, int32_t threadNum) {
//     int64_t begT = TimeUtility::currentTime();
//     AccessInfoCollectorPtr collector(
//             new AccessInfoCollector("topic1_aaaaaaaa", 0, "1.1", "java", "ea119", "sendMessage"));
//     collector->responseLength = 1000;
//     collector->requestLength = 100;
//     BrokerMetricsReporter reporter;
//     if (threadNum < 1) {
//         threadNum = 1;
//     }
//     size_t perCount = count/threadNum;
//     vector <autil::ThreadPtr> threadVec;
//     for (size_t i = 0; i < threadNum; i++) {
//         auto thread = autil::Thread::createThread(std::function<void()>([&]() {
//                                                                             for (size_t j = 0; j < perCount; j++) {
//                                                                                 reporter.reportAccessInfoMetrics(*collector);
//                                                                             }
//                                                                         }), "metrics");
//         threadVec.push_back(thread);
//     }
//     for (size_t i = 0 ; i < threadVec.size(); i++) {
//         threadVec[i]->join();
//     }

//     int64_t endT = TimeUtility::currentTime();
//     cout << "report multi thread: " << threadNum << ", count: " << count << ", time: " << (endT - begT) / 1000 <<
//     "ms"
//          << endl;
// }

// TEST_F(BrokerMetricsReporterTest, testReportMultiThread) {
//     size_t count = 2000000;
//     reportMutliThread(count, 1);
//     reportMutliThread(count, 2);
//     // reportMutliThread(count, 5);
//     // reportMutliThread(count, 10);
// }

}; // namespace monitor
}; // namespace swift
