#include "swift/admin/TopicInStatusManager.h"

#include <cstdint>
#include <iosfwd>
#include <map>
#include <string>
#include <type_traits>
#include <utility>

#include "autil/Log.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/test/TopicInStatusTestHelper.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/CircularVector.h"
#include "unittest/unittest.h"

using namespace swift::protocol;
using namespace std;

namespace swift {
namespace admin {

class TopicInStatusManagerTest : public TESTBASE {
public:
    TopicInStatusManagerTest() {}
    ~TopicInStatusManagerTest() {}

public:
    void setUp() {}
    void tearDown() {}

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, TopicInStatusManagerTest);

TEST_F(TopicInStatusManagerTest, testAddPartInMetric) {
    TopicInStatus tistatus;
    EXPECT_EQ(0, tistatus._rwMetrics.size());
    PartitionInStatus status;
    set_part_in_status(status, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100);

    EXPECT_TRUE(tistatus.addPartInMetric(status));
    EXPECT_EQ(1, tistatus._rwMetrics.size());
    const PartitionInStatus &back = tistatus._rwMetrics.back();
    ASSERT_NO_FATAL_FAILURE(expect_part_in_status(back, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 1));

    set_part_in_status(status, 2, 1200, 2200, 3200, 4200, 5200, 6200, 7200, 8200, 9200, 500);
    EXPECT_TRUE(tistatus.addPartInMetric(status, 9));
    const PartitionInStatus &back2 = tistatus._rwMetrics.back();
    ASSERT_NO_FATAL_FAILURE(expect_part_in_status(back2, 2, 1200, 2200, 3200, 4200, 5200, 6200, 7200, 8200, 9200, 7));
}

TEST_F(TopicInStatusManagerTest, testAddPartInMetric2) {
    TopicInStatusManager manager;
    EXPECT_EQ(0, manager._topicRWTimeMap.size());
    BrokerInMetric bMetric;
    PartitionInMetric pMetric;
    pMetric.set_topicname("topic_n");
    pMetric.set_lastwritetime(100);
    pMetric.set_lastreadtime(101);
    *bMetric.add_partinmetrics() = pMetric;
    pMetric.set_topicname("topic_l-12345-5");
    pMetric.set_lastwritetime(200);
    pMetric.set_lastreadtime(202);
    *bMetric.add_partinmetrics() = pMetric;
    bMetric.set_reporttime(300);

    manager.addPartInMetric(bMetric.partinmetrics(), bMetric.reporttime(), 1000, 2000);
    EXPECT_EQ(3, manager._topicRWTimeMap.size());
    auto iter = manager._topicRWTimeMap.find("topic_l-12345-5");
    EXPECT_TRUE(iter != manager._topicRWTimeMap.end());
    EXPECT_EQ(200, iter->second.first);
    EXPECT_EQ(202, iter->second.second);
    iter = manager._topicRWTimeMap.find("topic_l");
    EXPECT_TRUE(iter != manager._topicRWTimeMap.end());
    EXPECT_EQ(200, iter->second.first);
    EXPECT_EQ(202, iter->second.second);
}

TEST_F(TopicInStatusManagerTest, testTopicWriteReadTime) {
    TopicInStatusManager manager;
    TopicRWInfos rwInfos, result;
    manager.updateTopicWriteReadTime(rwInfos);
    EXPECT_TRUE(manager.getTopicWriteReadTime(string(), result));
    EXPECT_EQ(0, result.infos_size());

    TopicRWInfo *info = rwInfos.add_infos();
    info->set_topicname("topic1");
    info->set_lastwritetime(100);
    info->set_lastreadtime(200);
    info = rwInfos.add_infos();
    info->set_topicname("topic2");
    info->set_lastwritetime(300);
    info->set_lastreadtime(400);
    manager.updateTopicWriteReadTime(rwInfos);
    result.clear_infos();
    EXPECT_TRUE(manager.getTopicWriteReadTime(string(), result));
    EXPECT_EQ(2, result.infos_size());
    EXPECT_EQ("topic1", result.infos(0).topicname());
    EXPECT_EQ(100, result.infos(0).lastwritetime());
    EXPECT_EQ(200, result.infos(0).lastreadtime());
    EXPECT_EQ("topic2", result.infos(1).topicname());
    EXPECT_EQ(300, result.infos(1).lastwritetime());
    EXPECT_EQ(400, result.infos(1).lastreadtime());

    result.clear_infos();
    EXPECT_TRUE(manager.getTopicWriteReadTime("topic2", result));
    EXPECT_EQ(1, result.infos_size());
    EXPECT_EQ("topic2", result.infos(0).topicname());
    EXPECT_EQ(300, result.infos(0).lastwritetime());
    EXPECT_EQ(400, result.infos(0).lastreadtime());

    result.clear_infos();
    EXPECT_FALSE(manager.getTopicWriteReadTime("topic_not_exist", result));
    EXPECT_EQ(0, result.infos_size());
}

TEST_F(TopicInStatusManagerTest, testUpdateTopics) {
    TopicInStatusManager manager;
    PartitionInStatus status;

    set_part_in_status(status, 1, 11, 12, 13, 14, 15, 16, 17, 18, 19);
    TopicInStatus &tiStatus1 = manager._topicStatusMap["topic1"];
    tiStatus1.addPartInMetric(status);
    set_part_in_status(status, 2, 21, 22, 23, 24, 25, 26, 27, 28, 29);
    TopicInStatus &tiStatus2 = manager._topicStatusMap["topic2"];
    tiStatus2.addPartInMetric(status);
    set_part_in_status(status, 3, 31, 32, 33, 34, 35, 36, 37, 38, 39);
    TopicInStatus &tiStatus3 = manager._topicStatusMap["topic3"];
    tiStatus3.addPartInMetric(status);
    set_part_in_status(status, 4, 41, 42, 43, 44, 45, 46, 47, 48, 49);
    TopicInStatus &tiStatus4 = manager._topicStatusMap["topic4"];
    tiStatus4.addPartInMetric(status);

    manager._topicRWTimeMap["topic1"] = make_pair(111, 112);
    manager._topicRWTimeMap["topic2"] = make_pair(221, 222);
    manager._topicRWTimeMap["topic4"] = make_pair(441, 442);
    manager._topicRWTimeMap["topic5"] = make_pair(551, 552);

    TopicMap topicMap;
    topicMap["topic1"] = nullptr;
    topicMap["topic2"] = nullptr;
    topicMap["topic3"] = nullptr;
    topicMap["topic4"] = nullptr;
    topicMap["topic5"] = nullptr;

    manager.updateTopics(topicMap);
    EXPECT_EQ(4, manager._topicStatusMap.size());
    EXPECT_EQ(4, manager._topicRWTimeMap.size());

    topicMap.erase("topic1");
    topicMap.erase("topic3");
    manager.updateTopics(topicMap);
    EXPECT_EQ(2, manager._topicStatusMap.size());
    EXPECT_TRUE(manager._topicStatusMap.end() == manager._topicStatusMap.find("topic1"));
    EXPECT_TRUE(manager._topicStatusMap.end() != manager._topicStatusMap.find("topic2"));
    EXPECT_TRUE(manager._topicStatusMap.end() == manager._topicStatusMap.find("topic3"));
    EXPECT_TRUE(manager._topicStatusMap.end() != manager._topicStatusMap.find("topic4"));
    EXPECT_EQ(3, manager._topicRWTimeMap.size());
    EXPECT_TRUE(manager._topicRWTimeMap.end() == manager._topicRWTimeMap.find("topic1"));
    EXPECT_TRUE(manager._topicRWTimeMap.end() != manager._topicRWTimeMap.find("topic2"));
    EXPECT_TRUE(manager._topicRWTimeMap.end() != manager._topicRWTimeMap.find("topic4"));
    EXPECT_TRUE(manager._topicRWTimeMap.end() != manager._topicRWTimeMap.find("topic5"));

    topicMap.erase("topic2");
    topicMap.erase("topic5");
    manager.updateTopics(topicMap);
    EXPECT_EQ(1, manager._topicStatusMap.size());
    EXPECT_TRUE(manager._topicStatusMap.end() != manager._topicStatusMap.find("topic4"));
    EXPECT_EQ(1, manager._topicRWTimeMap.size());
    EXPECT_TRUE(manager._topicRWTimeMap.end() != manager._topicRWTimeMap.find("topic4"));

    topicMap.erase("topic4");
    manager.updateTopics(topicMap);
    EXPECT_EQ(0, manager._topicStatusMap.size());
    EXPECT_EQ(0, manager._topicRWTimeMap.size());
}

TEST_F(TopicInStatusManagerTest, testCalcResource) {
    TopicInStatus tis;
    EXPECT_EQ(1, tis.calcResource(0, 0, 0, 1));
    EXPECT_EQ(1, tis.calcResource(0, 0, 0, 1000000));
    uint32_t expectResource = (1000 + 2000 * 2 + 45 * 3000) / 6000; // 23
    EXPECT_EQ(expectResource, tis.calcResource(1000, 2000, 3000, 1));
    EXPECT_EQ(22, tis.calcResource(1000, 2000, 3000, 1, 22));
    EXPECT_EQ(expectResource, tis.calcResource(1000, 2000, 3000, 1, 23));
    EXPECT_EQ(expectResource, tis.calcResource(1000, 2000, 3000, 1, -1));
    EXPECT_EQ(expectResource, tis.calcResource(1000, 2000, 3000, 1, 0));
}

TEST_F(TopicInStatusManagerTest, testGetLatestResourceAvg) {
    TopicInStatus tis;
    PartitionInStatus pis;
    pis.resource = 1;
    tis._rwMetrics.push_back(pis);
    EXPECT_EQ(1, tis.getLatestResourceAvg());

    pis.resource = 3;
    tis._rwMetrics.push_back(pis);
    EXPECT_EQ(2, tis.getLatestResourceAvg());
    EXPECT_EQ(2, tis.getLatestResourceAvg(1));

    pis.resource = 0;
    tis._rwMetrics.push_back(pis);
    EXPECT_EQ(2, tis.getLatestResourceAvg());
    EXPECT_EQ(3, tis.getLatestResourceAvg(1));

    pis.resource = 5;
    tis._rwMetrics.push_back(pis);
    EXPECT_EQ(3, tis.getLatestResourceAvg());

    pis.resource = 7;
    tis._rwMetrics.push_back(pis);
    EXPECT_EQ(4, tis.getLatestResourceAvg());

    pis.resource = 6;
    tis._rwMetrics.push_back(pis);
    EXPECT_EQ(5, tis.getLatestResourceAvg());
}

TEST_F(TopicInStatusManagerTest, testGetPartitionResource) {
    TopicInStatusManager manager;
    TopicInStatus &tis = manager._topicStatusMap["topic"];

    uint32_t resource = 0;
    EXPECT_TRUE(manager.getPartitionResource("topic", resource));
    EXPECT_EQ(1, resource);

    PartitionInStatus pis;
    pis.resource = 3;
    tis._rwMetrics.push_back(pis);
    EXPECT_TRUE(manager.getPartitionResource("topic", resource));
    EXPECT_EQ(3, resource);

    EXPECT_FALSE(manager.getPartitionResource("topicNotExist", resource));
}

} // namespace admin
} // namespace swift
