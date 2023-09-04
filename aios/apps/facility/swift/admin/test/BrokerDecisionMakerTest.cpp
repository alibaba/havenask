#include "swift/admin/BrokerDecisionMaker.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/admin/RoleInfo.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/admin/test/MessageConstructor.h"
#include "swift/common/PathDefine.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "unittest/unittest.h"

namespace swift {
namespace admin {

class BrokerDecisionMakerTest : public TESTBASE {
public:
    BrokerDecisionMakerTest();
    ~BrokerDecisionMakerTest();

public:
    void setUp();
    void tearDown();

protected:
    WorkerInfoPtr constructBroker(const std::string &address, const std::string &topicName, uint32_t partition);
    WorkerInfoPtr constructBroker(const std::string &address,
                                  const std::vector<std::string> &topicNames,
                                  const std::vector<uint32_t> &partitions);
    TopicInfoPtr createTopicInfo(std::string topicName,
                                 uint32_t partitionCount,
                                 uint32_t resource = 100,
                                 uint32_t partitionLimit = 100000,
                                 std::string topicGroup = "");

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, BrokerDecisionMakerTest);

using namespace std;
using namespace swift::protocol;

BrokerDecisionMakerTest::BrokerDecisionMakerTest() {}

BrokerDecisionMakerTest::~BrokerDecisionMakerTest() {}

void BrokerDecisionMakerTest::setUp() { AUTIL_LOG(DEBUG, "setUp!"); }

void BrokerDecisionMakerTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

TEST_F(BrokerDecisionMakerTest, testGetOrderedBroker) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    RoleInfo roleInfo;
    WorkerMap workerMap;
    WorkerInfoPtr worker1(new WorkerInfo(roleInfo));
    WorkerInfoPtr worker2(new WorkerInfo(roleInfo));
    WorkerInfoPtr worker3(new WorkerInfo(roleInfo));
    worker1->addTask2target("t", 0, 100, 100);
    worker2->addTask2target("t", 0, 50, 100);
    worker3->addTask2target("t", 0, 70, 100);
    workerMap["worker1"] = worker1;
    workerMap["worker2"] = worker2;
    workerMap["worker3"] = worker3;

    BrokerDecisionMaker::OrderedBroker broker;
    BrokerDecisionMaker decisionMaker;
    decisionMaker.getOrderedBroker(workerMap, broker);

    ASSERT_EQ((size_t)3, broker.size());
    WorkerInfoPtr worker = *(broker.begin());
    broker.erase(broker.begin());
    ASSERT_EQ((uint32_t)9950, worker->getFreeResource());

    worker = *(broker.begin());
    broker.erase(broker.begin());
    ASSERT_EQ((uint32_t)9930, worker->getFreeResource());

    worker = *(broker.begin());
    broker.erase(broker.begin());
    ASSERT_EQ((uint32_t)9900, worker->getFreeResource());

    ASSERT_TRUE(broker.empty());
}

TEST_F(BrokerDecisionMakerTest, testGetPartitionPairList) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    TopicMap topicMap;
    topicMap["t1"] = createTopicInfo("t1", 2, 20);
    topicMap["t2"] = createTopicInfo("t2", 1, 50);
    topicMap["t3"] = createTopicInfo("t3", 1, 20);

    PartitionPairList partitions;
    BrokerDecisionMaker decisionMaker;
    decisionMaker.getPartitionPairList(topicMap, partitions);

    ASSERT_TRUE(!partitions.empty());
    PartitionPair partition = *(partitions.begin());
    partitions.pop_front();
    ASSERT_EQ(string("t2"), (partition.first)->getTopicName());
    ASSERT_EQ((uint32_t)0, partition.second);

    partition = *(partitions.begin());
    partitions.pop_front();
    ASSERT_EQ(string("t3"), (partition.first)->getTopicName());
    ASSERT_EQ((uint32_t)0, partition.second);

    partition = *(partitions.begin());
    partitions.pop_front();
    ASSERT_EQ(string("t1"), (partition.first)->getTopicName());
    ASSERT_EQ((uint32_t)0, partition.second);

    partition = *(partitions.begin());
    partitions.pop_front();
    ASSERT_EQ(string("t1"), (partition.first)->getTopicName());
    ASSERT_EQ((uint32_t)1, partition.second);

    ASSERT_TRUE(partitions.empty());
}

TEST_F(BrokerDecisionMakerTest, testReuseFormerlyAssignedBroker) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    BrokerDecisionMaker decisionMaker;
    {
        /* [t1, 0] -> b1
           [t1, 1] -> b1
           [t2, 0] -> b2
           [t2, 1] -> b2
           b1 -> [t1, 0], [t1, 1]
           b2 -> empty
        */
        TopicInfoPtr topicInfoPtr1 = createTopicInfo("t1", 2, 6000);
        TopicInfoPtr topicInfoPtr2 = createTopicInfo("t2", 2, 6000);
        WorkerMap broker;

        vector<string> topicNames;
        vector<uint32_t> partitions;
        topicNames.push_back("t1");
        topicNames.push_back("t1");
        partitions.push_back(0);
        partitions.push_back(1);
        string role1 = "b1";
        string address1 = "b1#_#10.10.10.10:101";
        broker[role1] = constructBroker(address1, topicNames, partitions);
        string role2 = "b2";
        string address2 = "b2#_#10.10.10.11:102";
        broker[role2] = constructBroker(address2, "", 0);

        topicInfoPtr1->setCurrentRoleAddress(0, address1);
        topicInfoPtr1->setCurrentRoleAddress(1, address1);
        topicInfoPtr2->setCurrentRoleAddress(0, address2);
        topicInfoPtr2->setCurrentRoleAddress(1, address2);

        PartitionPairList partitionPairs;
        partitionPairs.push_back(make_pair(topicInfoPtr1, 0));
        partitionPairs.push_back(make_pair(topicInfoPtr1, 1));
        partitionPairs.push_back(make_pair(topicInfoPtr2, 0));
        partitionPairs.push_back(make_pair(topicInfoPtr2, 1));
        decisionMaker.reuseFormerlyAssignedBroker(partitionPairs, broker, false);
        // [t1, 1], [t2, 0], [t2, 1] can't assign to broker
        ASSERT_EQ((size_t)3, partitionPairs.size());

        ASSERT_EQ(address1, topicInfoPtr1->getTargetRoleAddress(0));
        ASSERT_EQ(string(""), topicInfoPtr1->getTargetRoleAddress(1));
        ASSERT_EQ(string(""), topicInfoPtr2->getTargetRoleAddress(0));
        ASSERT_EQ(string(""), topicInfoPtr2->getTargetRoleAddress(1));
        WorkerInfo::TaskSet taskSet;
        taskSet = broker[role1]->getTargetTask();
        ASSERT_EQ((size_t)1, taskSet.size());
        ASSERT_TRUE(taskSet.find(make_pair("t1", 0)) != taskSet.end());

        taskSet = broker[role2]->getTargetTask();
        ASSERT_EQ((size_t)0, taskSet.size());
    }
    {
        /* [t1, 0] -> b1
           [t1, 1] -> b1
           [t2, 0] -> b2
           [t2, 1] -> b2
           b1 -> [t1, 0], [t1, 1]
           b2 -> empty
        */
        TopicInfoPtr topicInfoPtr1 = createTopicInfo("t1", 2, 6000);
        TopicInfoPtr topicInfoPtr2 = createTopicInfo("t2", 2, 6000);
        WorkerMap broker;

        vector<string> topicNames;
        vector<uint32_t> partitions;
        topicNames.push_back("t1");
        topicNames.push_back("t1");
        partitions.push_back(0);
        partitions.push_back(1);
        string role1 = "b1";
        string address1 = "b1#_#10.10.10.10:101";
        broker[role1] = constructBroker(address1, topicNames, partitions);
        string role2 = "b2";
        string address2 = "b2#_#10.10.10.11:102";
        broker[role2] = constructBroker(address2, "", 0);

        topicInfoPtr1->setCurrentRoleAddress(0, address1);
        topicInfoPtr1->setCurrentRoleAddress(1, address1);
        topicInfoPtr2->setCurrentRoleAddress(0, address2);
        topicInfoPtr2->setCurrentRoleAddress(1, address2);

        PartitionPairList partitionPairs;
        partitionPairs.push_back(make_pair(topicInfoPtr1, 0));
        partitionPairs.push_back(make_pair(topicInfoPtr1, 1));
        partitionPairs.push_back(make_pair(topicInfoPtr2, 0));
        partitionPairs.push_back(make_pair(topicInfoPtr2, 1));
        decisionMaker.reuseFormerlyAssignedBroker(partitionPairs, broker, true);
        // [t1, 1], [t2, 1] can't assign to broker
        ASSERT_EQ((size_t)2, partitionPairs.size());

        ASSERT_EQ(address1, topicInfoPtr1->getTargetRoleAddress(0));
        ASSERT_EQ(string(""), topicInfoPtr1->getTargetRoleAddress(1));
        ASSERT_EQ(address2, topicInfoPtr2->getTargetRoleAddress(0));
        ASSERT_EQ(string(""), topicInfoPtr2->getTargetRoleAddress(1));
        WorkerInfo::TaskSet taskSet;
        taskSet = broker[role1]->getTargetTask();
        ASSERT_EQ((size_t)1, taskSet.size());
        ASSERT_TRUE(taskSet.find(make_pair("t1", 0)) != taskSet.end());

        taskSet = broker[role2]->getTargetTask();
        ASSERT_EQ((size_t)1, taskSet.size());
        ASSERT_TRUE(taskSet.find(make_pair("t2", 0)) != taskSet.end());
    }
    {
        TopicInfoPtr topicInfoPtr1 = createTopicInfo("t1", 2, 300);
        WorkerMap broker;
        vector<string> topicNames;
        vector<uint32_t> partitions;
        topicNames.push_back("t1");
        topicNames.push_back("t1");
        partitions.push_back(0);
        partitions.push_back(1);
        string role1 = "b1";
        string address1 = "b1#_#10.10.10.10:101";
        broker[role1] = constructBroker(address1, topicNames, partitions);

        topicInfoPtr1->setCurrentRoleAddress(0, ""); // p_0 role name is empty
        topicInfoPtr1->setCurrentRoleAddress(1, address1);

        PartitionPairList partitionPairs;
        partitionPairs.push_back(make_pair(topicInfoPtr1, 0));
        partitionPairs.push_back(make_pair(topicInfoPtr1, 1));
        decisionMaker.reuseFormerlyAssignedBroker(partitionPairs, broker, true);
        ASSERT_EQ((size_t)1, partitionPairs.size());
        ASSERT_EQ(string(""), topicInfoPtr1->getTargetRoleAddress(0));
        ASSERT_EQ(address1, topicInfoPtr1->getTargetRoleAddress(1));

        WorkerInfo::TaskSet taskSet;
        taskSet = broker[role1]->getTargetTask();
        ASSERT_EQ((size_t)1, taskSet.size());
        ASSERT_TRUE(taskSet.find(make_pair("t1", 1)) != taskSet.end());
    }
    {
        TopicInfoPtr topicInfoPtr1 = createTopicInfo("t1", 2, 300);
        WorkerMap broker;
        vector<string> topicNames;
        vector<uint32_t> partitions;
        topicNames.push_back("t1");
        topicNames.push_back("t1");
        partitions.push_back(0);
        partitions.push_back(1);
        string role1 = "b1";
        string address1 = "b1#_#10.10.10.10:101";
        broker[role1] = constructBroker(address1, topicNames, partitions);

        topicInfoPtr1->setCurrentRoleAddress(0, "b2#_#10.10.10.10:101"); // p_0 role name is not exist
        topicInfoPtr1->setCurrentRoleAddress(1, address1);

        PartitionPairList partitionPairs;
        partitionPairs.push_back(make_pair(topicInfoPtr1, 0));
        partitionPairs.push_back(make_pair(topicInfoPtr1, 1));
        decisionMaker.reuseFormerlyAssignedBroker(partitionPairs, broker, true);
        ASSERT_EQ((size_t)1, partitionPairs.size());
        ASSERT_EQ(string(""), topicInfoPtr1->getTargetRoleAddress(0));
        ASSERT_EQ(address1, topicInfoPtr1->getTargetRoleAddress(1));

        WorkerInfo::TaskSet taskSet;
        taskSet = broker[role1]->getTargetTask();
        ASSERT_EQ((size_t)1, taskSet.size());
        ASSERT_TRUE(taskSet.find(make_pair("t1", 1)) != taskSet.end());
    }
}
TEST_F(BrokerDecisionMakerTest, testReuseCurrentExistPartition) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    BrokerDecisionMaker decisionMaker;
    TopicMap topicMap;
    TopicInfoPtr topicInfoPtr(createTopicInfo("t1", 2, 70));
    topicMap["t1"] = topicInfoPtr;
    WorkerMap broker;
    string role = "b1";
    string address = "b1#_#10.10.10.10:101";
    broker[role] = constructBroker(address, "t1", 1);

    ASSERT_EQ(string(""), topicInfoPtr->getTargetRoleAddress(0));
    ASSERT_EQ(string(""), topicInfoPtr->getTargetRoleAddress(1));

    decisionMaker.reuseCurrentExistPartition(topicMap, broker);

    ASSERT_EQ(string(""), topicInfoPtr->getTargetRoleAddress(0));
    ASSERT_EQ(address, topicInfoPtr->getTargetRoleAddress(1));
}

TEST_F(BrokerDecisionMakerTest, testReuseCurrentExistPartitionWhenPartitionPlus) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    BrokerDecisionMaker decisionMaker;
    TopicMap topicMap;
    TopicInfoPtr topicInfoPtr(createTopicInfo("t1", 2, 70));
    topicMap["t1"] = topicInfoPtr;
    WorkerMap broker;
    string role0 = "b0";
    string address0 = "b0#_#10.10.10.10:100";
    string role1 = "b1";
    string address1 = "b1#_#10.10.10.11:101";
    string role2 = "b2";
    string address2 = "b2#_#10.10.10.12:102";
    string role3 = "b3";
    string address3 = "b3#_#10.10.10.12:102";

    broker[role0] = constructBroker(address0, "t1", 0);
    broker[role1] = constructBroker(address1, "t1", 1);
    broker[role2] = constructBroker(address2, "t1", 2);
    broker[role3] = constructBroker(address3, "t2", 0);

    ASSERT_EQ(string(""), topicInfoPtr->getTargetRoleAddress(0));
    ASSERT_EQ(string(""), topicInfoPtr->getTargetRoleAddress(1));

    decisionMaker.reuseCurrentExistPartition(topicMap, broker);
    ASSERT_EQ(address0, topicInfoPtr->getTargetRoleAddress(0));
    ASSERT_EQ(address1, topicInfoPtr->getTargetRoleAddress(1));
}

TEST_F(BrokerDecisionMakerTest, testNotReuseCurrentExistPartitionWhichHadBeenDispatchedToOtherBroker) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    BrokerDecisionMaker decisionMaker;
    // we have topic t1 with 2 partition which has no target borker
    TopicMap topicMap;
    TopicInfoPtr topicInfoPtr(createTopicInfo("t1", 2, 70));
    topicMap["t1"] = topicInfoPtr;

    // partition 0 has been dispatched to some broker
    // and with that broker's death, partition 0 has no target broker now
    topicInfoPtr->setTargetRoleAddress(0, "somebroker#_#10.10.10.10:101");
    topicInfoPtr->setTargetRoleAddress(0, "");
    ASSERT_EQ(string(""), topicInfoPtr->getTargetRoleAddress(0));
    ASSERT_EQ(string(""), topicInfoPtr->getTargetRoleAddress(1));

    // now we have broker b1 which claims that he has loaded 0 and 1 partition of topic t1
    WorkerMap broker;
    string role = "b1";
    string address = "b1#_#10.10.10.10:101";
    vector<string> topicNames;
    topicNames.push_back("t1");
    topicNames.push_back("t1");
    vector<uint32_t> partitions;
    partitions.push_back(0);
    partitions.push_back(1);
    broker[role] = constructBroker(address, topicNames, partitions);

    decisionMaker.reuseCurrentExistPartition(topicMap, broker);

    // only partition 1 will be reused in b1 since partition 0 has been dispatched
    // to some other broker, we need b1 to unload partition 0 before load it
    ASSERT_EQ(string(""), topicInfoPtr->getTargetRoleAddress(0));
    ASSERT_EQ(address, topicInfoPtr->getTargetRoleAddress(1));
}

TEST_F(BrokerDecisionMakerTest, testAssignPartitionToBroker) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    BrokerDecisionMaker decisionMaker;
    TopicInfoPtr topicInfoPtr1 = createTopicInfo("t1", 2, 7000);
    TopicInfoPtr topicInfoPtr2 = createTopicInfo("t2", 2, 4000);
    WorkerMap broker;

    string role1 = "b1";
    string address1 = "b1#_#10.10.10.10:101";
    broker[role1] = constructBroker(address1, "t1", 0);
    ASSERT_TRUE(broker[role1]->addTask2target("t1", 0, 7000, 100));

    string role2 = "b2";
    string address2 = "b2#_#10.10.10.11:102";
    broker[role2] = constructBroker(address2, "t2", 0);
    ASSERT_TRUE(broker[role2]->addTask2target("t2", 0, 4000, 100));

    PartitionPairList partitionPairs;
    partitionPairs.push_back(make_pair(topicInfoPtr1, 0));
    partitionPairs.push_back(make_pair(topicInfoPtr1, 1));
    partitionPairs.push_back(make_pair(topicInfoPtr2, 1));

    decisionMaker.assignPartitionToBroker(partitionPairs, broker);

    // [t1, 0] exist in b1
    ASSERT_EQ(string(""), topicInfoPtr1->getTargetRoleAddress(0));

    // not enough resoure for [t1, 1] in bouth b1 and b2
    ASSERT_EQ(string(""), topicInfoPtr1->getTargetRoleAddress(1));

    // [t2, 1] assigned to b2
    ASSERT_EQ(address2, topicInfoPtr2->getTargetRoleAddress(1));

    WorkerInfo::TaskSet taskSet;
    taskSet = broker[role1]->getTargetTask();
    ASSERT_EQ((size_t)1, taskSet.size());
    ASSERT_TRUE(taskSet.find(make_pair("t1", 0)) != taskSet.end());

    taskSet = broker[role2]->getTargetTask();
    ASSERT_EQ((size_t)2, taskSet.size());
    ASSERT_TRUE(taskSet.find(make_pair("t2", 0)) != taskSet.end());
    ASSERT_TRUE(taskSet.find(make_pair("t2", 1)) != taskSet.end());
}

TEST_F(BrokerDecisionMakerTest, testAssignPartitionToBrokerWithPartitionLimit) {
    BrokerDecisionMaker decisionMaker;
    TopicInfoPtr topicInfoPtr1(createTopicInfo("t1", 2, 40, 1));
    TopicInfoPtr topicInfoPtr2(createTopicInfo("t2", 4, 30, 2));
    WorkerMap broker;

    string role1 = "b1";
    string address1 = "b1#_#10.10.10.10:101";
    broker[role1] = constructBroker(address1, topicInfoPtr1->getTopicName(), 0);
    ASSERT_TRUE(broker[role1]->addTask2target(
        topicInfoPtr1->getTopicName(), 0, topicInfoPtr1->getResource(), topicInfoPtr1->getPartitionLimit()));

    string role2 = "b2";
    string address2 = "b2#_#10.10.10.11:102";
    broker[role2] = constructBroker(address2, topicInfoPtr2->getTopicName(), 1);
    ASSERT_TRUE(broker[role2]->addTask2target(
        topicInfoPtr2->getTopicName(), 0, topicInfoPtr2->getResource(), topicInfoPtr2->getPartitionLimit()));
    ASSERT_TRUE(broker[role2]->addTask2target(
        topicInfoPtr2->getTopicName(), 2, topicInfoPtr2->getResource(), topicInfoPtr2->getPartitionLimit()));

    PartitionPairList partitionPairs;
    partitionPairs.push_back(make_pair(topicInfoPtr1, 1));
    partitionPairs.push_back(make_pair(topicInfoPtr2, 1));
    partitionPairs.push_back(make_pair(topicInfoPtr2, 3));

    decisionMaker.assignPartitionToBroker(partitionPairs, broker);

    // b1's free resource greater than b2, but b1 reach the limit of t1 partiton
    ASSERT_EQ(address2, topicInfoPtr1->getTargetRoleAddress(1));

    // [t2, 1] exist in b2, will not assign
    ASSERT_EQ(string(""), topicInfoPtr2->getTargetRoleAddress(1));

    // [t2, 3] assigned to b1
    ASSERT_EQ(address1, topicInfoPtr2->getTargetRoleAddress(3));

    WorkerInfo::TaskSet taskSet;
    taskSet = broker[role1]->getTargetTask();
    ASSERT_EQ((size_t)2, taskSet.size());

    ASSERT_TRUE(taskSet.find(make_pair("t1", 0)) != taskSet.end());
    ASSERT_TRUE(taskSet.find(make_pair("t2", 3)) != taskSet.end());

    taskSet = broker[role2]->getTargetTask();
    ASSERT_EQ((size_t)3, taskSet.size());
    ASSERT_TRUE(taskSet.find(make_pair("t1", 1)) != taskSet.end());
    ASSERT_TRUE(taskSet.find(make_pair("t2", 0)) != taskSet.end());
    ASSERT_TRUE(taskSet.find(make_pair("t2", 2)) != taskSet.end());
}

TEST_F(BrokerDecisionMakerTest, testAssignPartitionToBrokerWithGroupLimit) {
    BrokerDecisionMaker decisionMaker;
    {
        TopicInfoPtr topicInfoPtr1(createTopicInfo("t1", 2, 40, 10, "group1"));
        WorkerMap broker;
        string role1 = "not_exist_group##b1";
        string address1 = "not_exist_group##b1#_#10.10.10.10:101";
        broker[role1] = constructBroker(address1, topicInfoPtr1->getTopicName(), 0);
        ASSERT_FALSE(broker[role1]->addTask2target(topicInfoPtr1->getTopicName(),
                                                   0,
                                                   topicInfoPtr1->getResource(),
                                                   topicInfoPtr1->getPartitionLimit(),
                                                   topicInfoPtr1->getTopicGroup()));
    }
    {
        TopicInfoPtr topicInfoPtr1(createTopicInfo("t1", 2, 6000, 10, "group1"));
        TopicInfoPtr topicInfoPtr2(createTopicInfo("t2", 2, 1000, 10, "group2"));
        WorkerMap broker;
        string role1 = "group1##b1";
        string address1 = "group1##b1#_#10.10.10.10:101";
        broker[role1] = constructBroker(address1, topicInfoPtr1->getTopicName(), 0);
        ASSERT_TRUE(broker[role1]->addTask2target(topicInfoPtr1->getTopicName(),
                                                  0,
                                                  topicInfoPtr1->getResource(),
                                                  topicInfoPtr1->getPartitionLimit(),
                                                  topicInfoPtr1->getTopicGroup()));
        string role2 = "group2##b2";
        string address2 = "group2##b2#_#10.10.10.11:102";
        broker[role2] = constructBroker(address2, topicInfoPtr2->getTopicName(), 1);

        PartitionPairList partitionPairs;
        partitionPairs.push_back(make_pair(topicInfoPtr1, 1));
        partitionPairs.push_back(make_pair(topicInfoPtr2, 0));
        partitionPairs.push_back(make_pair(topicInfoPtr2, 1));

        decisionMaker.assignPartitionToBroker(partitionPairs, broker);

        // b2's free resource is enough, but group name not equal
        ASSERT_EQ(string(""), topicInfoPtr1->getTargetRoleAddress(1));

        // topic2 loaded on addr2 normal
        ASSERT_EQ(address2, topicInfoPtr2->getTargetRoleAddress(0));

        // [t2, 1] exist in b2, will not assign
        ASSERT_EQ(string(""), topicInfoPtr2->getTargetRoleAddress(1));

        WorkerInfo::TaskSet taskSet;
        taskSet = broker[role1]->getTargetTask();
        ASSERT_EQ((size_t)1, taskSet.size());

        ASSERT_TRUE(taskSet.find(make_pair("t1", 0)) != taskSet.end());

        taskSet = broker[role2]->getTargetTask();
        ASSERT_EQ((size_t)1, taskSet.size());
        ASSERT_TRUE(taskSet.find(make_pair("t2", 0)) != taskSet.end());
    }
}

TEST_F(BrokerDecisionMakerTest, testMakeDecision) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    TopicMap topicMap;
    string topicName = "news_search";
    TopicInfoPtr topicInfoPtr = createTopicInfo(topicName, 7, 30);
    topicMap[topicName] = topicInfoPtr;

    WorkerMap workerMap;
    string role1 = "b1";
    string address1 = "b1#_#10.10.10.10:100";
    workerMap[role1] = constructBroker(address1, topicName, 1);
    string role2 = "b2";
    string address2 = "b2#_#10.10.10.11:101";
    workerMap[role2] = constructBroker(address2, "", 0);
    string role3 = "b3";
    string address3 = "b3#_#10.10.10.12:102";
    workerMap[role3] = constructBroker(address3, "not_exist", 1);
    string role4 = "b4";
    string address4 = "b4#_#10.10.10.13:103";
    workerMap[role4] = constructBroker(address4, topicName, 5);
    string role5 = "b5";
    string address5 = "b5#_#10.10.10.14:104";
    workerMap[role5] = constructBroker(address5, topicName, 1);

    topicInfoPtr->setCurrentRoleAddress(1, address1);
    topicInfoPtr->setCurrentRoleAddress(5, address4);
    topicInfoPtr->setCurrentRoleAddress(3, address2);

    BrokerDecisionMaker decisionMaker;
    decisionMaker.makeDecision(topicMap, workerMap);

    ASSERT_EQ(size_t(1), topicMap.size());
    TopicInfoPtr topicInfoPtr1 = topicMap[topicName];
    ASSERT_TRUE(topicInfoPtr1);
    ASSERT_EQ(uint32_t(7), topicInfoPtr1->getPartitionCount());

    ASSERT_EQ(address5, topicInfoPtr1->getTargetRoleAddress(0));
    ASSERT_EQ(address1, topicInfoPtr1->getTargetRoleAddress(1));
    ASSERT_EQ(address3, topicInfoPtr1->getTargetRoleAddress(2));
    ASSERT_EQ(address2, topicInfoPtr1->getTargetRoleAddress(3));
    ASSERT_EQ(address5, topicInfoPtr1->getTargetRoleAddress(4));
    ASSERT_EQ(address4, topicInfoPtr1->getTargetRoleAddress(5));
    ASSERT_EQ(address4, topicInfoPtr1->getTargetRoleAddress(6));

    WorkerInfo::TaskSet taskSet;

    taskSet = workerMap[role1]->getTargetTask();
    ASSERT_EQ((size_t)1, taskSet.size());
    ASSERT_TRUE(taskSet.find(make_pair(topicName, 1)) != taskSet.end());

    taskSet = workerMap[role2]->getTargetTask();
    ASSERT_EQ((size_t)1, taskSet.size());
    ASSERT_TRUE(taskSet.find(make_pair(topicName, 3)) != taskSet.end());

    taskSet = workerMap[role3]->getTargetTask();
    ASSERT_EQ((size_t)1, taskSet.size());
    ASSERT_TRUE(taskSet.find(make_pair(topicName, 2)) != taskSet.end());

    taskSet = workerMap[role4]->getTargetTask();
    ASSERT_EQ((size_t)2, taskSet.size());
    ASSERT_TRUE(taskSet.find(make_pair(topicName, 5)) != taskSet.end());
    ASSERT_TRUE(taskSet.find(make_pair(topicName, 6)) != taskSet.end());

    taskSet = workerMap[role5]->getTargetTask();
    ASSERT_EQ((size_t)2, taskSet.size());
    ASSERT_TRUE(taskSet.find(make_pair(topicName, 0)) != taskSet.end());
    ASSERT_TRUE(taskSet.find(make_pair(topicName, 4)) != taskSet.end());
}

WorkerInfoPtr
BrokerDecisionMakerTest::constructBroker(const string &roleAddress, const string &topicName, uint32_t partition) {
    string address;
    RoleInfo roleInfo;
    common::PathDefine::parseRoleAddress(roleAddress, roleInfo.roleName, address);
    common::PathDefine::parseAddress(address, roleInfo.ip, roleInfo.port);
    WorkerInfoPtr workerInfoPtr(new WorkerInfo(roleInfo));
    HeartbeatInfo heartbeatInfo =
        MessageConstructor::ConstructHeartbeatInfo(roleAddress, ROLE_TYPE_BROKER, true, topicName, partition);
    workerInfoPtr->setCurrent(heartbeatInfo);
    workerInfoPtr->prepareDecision(0, 0);
    return workerInfoPtr;
}

WorkerInfoPtr BrokerDecisionMakerTest::constructBroker(const string &roleAddress,
                                                       const vector<string> &topicNames,
                                                       const vector<uint32_t> &partitions) {
    assert(topicNames.size() == partitions.size());
    string address;
    RoleInfo roleInfo;
    common::PathDefine::parseRoleAddress(roleAddress, roleInfo.roleName, address);
    common::PathDefine::parseAddress(address, roleInfo.ip, roleInfo.port);
    WorkerInfoPtr workerInfoPtr(new WorkerInfo(roleInfo));
    vector<protocol::PartitionStatus> status;
    for (size_t i = 0; i < topicNames.size(); ++i) {
        status.push_back(protocol::PARTITION_STATUS_WAITING);
    }
    vector<int64_t> sessionIds;
    sessionIds.resize(topicNames.size(), -1);
    HeartbeatInfo heartbeatInfo = MessageConstructor::ConstructHeartbeatInfo(
        roleAddress, ROLE_TYPE_BROKER, true, topicNames, partitions, status, sessionIds);
    workerInfoPtr->setCurrent(heartbeatInfo);
    workerInfoPtr->prepareDecision(0, 0);
    return workerInfoPtr;
}

TopicInfoPtr BrokerDecisionMakerTest::createTopicInfo(
    std::string topicName, uint32_t partitionCount, uint32_t resource, uint32_t partitionLimit, string topicGroup) {
    TopicCreationRequest topicMeta;
    topicMeta.set_topicname(topicName);
    topicMeta.set_partitioncount(partitionCount);
    topicMeta.set_resource(resource);
    topicMeta.set_partitionlimit(partitionLimit);
    topicMeta.set_topicgroup(topicGroup);
    return TopicInfoPtr(new TopicInfo(topicMeta));
}

TEST_F(BrokerDecisionMakerTest, testVetical) {
    PartitionPairList partitionPairs;
    vector<TopicInfoPtr> tinfos;
    TopicInfoPtr d2 = createTopicInfo("d2", 2, 1, 1, "default");
    TopicInfoPtr d3 = createTopicInfo("d3", 3, 1, 1, "default");
    TopicInfoPtr d4 = createTopicInfo("d4", 4, 1, 2, "default");
    TopicInfoPtr d5 = createTopicInfo("d5", 5, 1, 2, "default");
    tinfos.push_back(d2);
    tinfos.push_back(d3);
    tinfos.push_back(d4);
    tinfos.push_back(d5);
    TopicInfoPtr o2 = createTopicInfo("o2", 2, 1, 1, "other");
    TopicInfoPtr o3 = createTopicInfo("o3", 3, 1, 2, "other");
    tinfos.push_back(o2);
    tinfos.push_back(o3);
    TopicInfoPtr n2 = createTopicInfo("n1", 2, 1, 1, "notexist");
    tinfos.push_back(n2);

    for (auto &topic : tinfos) {
        for (int pid = 0; pid < topic->getPartitionCount(); ++pid) {
            partitionPairs.push_back(make_pair(topic, pid));
        }
    }

    map<string, uint32_t> veticalGroupBrokerCnt;
    veticalGroupBrokerCnt["default"] = 3;
    veticalGroupBrokerCnt["other"] = 3;
    BrokerDecisionMaker decisionMaker("vetical", veticalGroupBrokerCnt);

    WorkerMap brokers;
    string rg0 = "default##broker_0_1";
    string gaddr0 = "default##broker_0_1#_#10.10.10.10:100";
    brokers[rg0] = constructBroker(gaddr0, "d2", 0);
    string rg1 = "default##broker_1_1";
    string gaddr1 = "default##broker_1_1#_#10.10.10.10:101";
    brokers[rg1] = constructBroker(gaddr1, "d3", 0);
    string rg2 = "default##broker_2_1";
    string gaddr2 = "default##broker_2_1#_#10.10.10.10:102";
    brokers[rg2] = constructBroker(gaddr2, "d4", 0);

    // oaddr1 dead
    string ro0 = "other##broker_0_1";
    string oaddr0 = "other##broker_0_1#_#10.10.10.11:100";
    brokers[ro0] = constructBroker(oaddr0, "t1", 0); // t1 not exist
    string ro2 = "other##broker_2_1";
    string oaddr2 = "other##broker_2_1#_#10.10.10.11:102";
    brokers[ro2] = constructBroker(oaddr2, "t1", 0); // t1 not exist

    auto partPairs = partitionPairs;
    decisionMaker.vetical(partPairs, brokers);

    EXPECT_EQ(gaddr0, d2->getTargetRoleAddress(0));
    EXPECT_EQ(gaddr1, d2->getTargetRoleAddress(1));
    EXPECT_EQ(gaddr0, d3->getTargetRoleAddress(0));
    EXPECT_EQ(gaddr1, d3->getTargetRoleAddress(1));
    EXPECT_EQ(gaddr2, d3->getTargetRoleAddress(2));
    EXPECT_EQ(gaddr0, d4->getTargetRoleAddress(0));
    EXPECT_EQ(gaddr1, d4->getTargetRoleAddress(1));
    EXPECT_EQ(gaddr2, d4->getTargetRoleAddress(2));
    EXPECT_EQ(gaddr0, d4->getTargetRoleAddress(3));
    EXPECT_EQ(gaddr0, d5->getTargetRoleAddress(0));
    EXPECT_EQ(gaddr1, d5->getTargetRoleAddress(1));
    EXPECT_EQ(gaddr2, d5->getTargetRoleAddress(2));
    EXPECT_EQ(gaddr0, d5->getTargetRoleAddress(3));
    EXPECT_EQ(gaddr1, d5->getTargetRoleAddress(4));

    EXPECT_EQ(oaddr0, o2->getTargetRoleAddress(0));
    EXPECT_EQ(oaddr2, o2->getTargetRoleAddress(1));
    EXPECT_EQ(oaddr0, o3->getTargetRoleAddress(0));
    EXPECT_EQ(oaddr2, o3->getTargetRoleAddress(1));
    EXPECT_EQ(oaddr2, o3->getTargetRoleAddress(2));

    EXPECT_EQ("", n2->getTargetRoleAddress(0));
    EXPECT_EQ("", n2->getTargetRoleAddress(1));

    WorkerInfo::TaskSet taskSet;
    taskSet = brokers[rg0]->getTargetTask();
    EXPECT_EQ(6, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("d2", 0)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d3", 0)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d4", 0)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d4", 3)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d5", 0)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d5", 3)) != taskSet.end());
    taskSet = brokers[rg1]->getTargetTask();
    EXPECT_EQ(5, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("d2", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d3", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d4", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d5", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d5", 4)) != taskSet.end());
    taskSet = brokers[rg2]->getTargetTask();
    EXPECT_EQ(3, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("d3", 2)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d5", 2)) != taskSet.end());

    taskSet = brokers[ro0]->getTargetTask();
    EXPECT_EQ(2, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("o2", 0)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("o3", 0)) != taskSet.end());
    taskSet = brokers[ro2]->getTargetTask();
    EXPECT_EQ(3, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("o2", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("o3", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("o3", 2)) != taskSet.end());

    // oaddr1 back again
    string ro1 = "other##broker_1_1";
    string oaddr1 = "other##broker_1_1#_#10.10.10.11:101";
    brokers[ro1] = constructBroker(oaddr1, "t1", 0);
    auto curTime = autil::TimeUtility::currentTime();
    for (auto &broker : brokers) {
        broker.second->prepareDecision(curTime, curTime);
    }
    partPairs = partitionPairs;
    decisionMaker.vetical(partPairs, brokers);

    EXPECT_EQ(gaddr0, d2->getTargetRoleAddress(0));
    EXPECT_EQ(gaddr1, d2->getTargetRoleAddress(1));
    EXPECT_EQ(gaddr0, d3->getTargetRoleAddress(0));
    EXPECT_EQ(gaddr1, d3->getTargetRoleAddress(1));
    EXPECT_EQ(gaddr2, d3->getTargetRoleAddress(2));
    EXPECT_EQ(gaddr0, d4->getTargetRoleAddress(0));
    EXPECT_EQ(gaddr1, d4->getTargetRoleAddress(1));
    EXPECT_EQ(gaddr2, d4->getTargetRoleAddress(2));
    EXPECT_EQ(gaddr0, d4->getTargetRoleAddress(3));
    EXPECT_EQ(gaddr0, d5->getTargetRoleAddress(0));
    EXPECT_EQ(gaddr1, d5->getTargetRoleAddress(1));
    EXPECT_EQ(gaddr2, d5->getTargetRoleAddress(2));
    EXPECT_EQ(gaddr0, d5->getTargetRoleAddress(3));
    EXPECT_EQ(gaddr1, d5->getTargetRoleAddress(4));

    EXPECT_EQ(oaddr0, o2->getTargetRoleAddress(0));
    EXPECT_EQ(oaddr1, o2->getTargetRoleAddress(1));
    EXPECT_EQ(oaddr0, o3->getTargetRoleAddress(0));
    EXPECT_EQ(oaddr1, o3->getTargetRoleAddress(1));
    EXPECT_EQ(oaddr2, o3->getTargetRoleAddress(2));

    taskSet = brokers[rg0]->getTargetTask();
    EXPECT_EQ(6, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("d2", 0)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d3", 0)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d4", 0)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d4", 3)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d5", 0)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d5", 3)) != taskSet.end());
    taskSet = brokers[rg1]->getTargetTask();
    EXPECT_EQ(5, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("d2", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d3", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d4", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d5", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d5", 4)) != taskSet.end());
    taskSet = brokers[rg2]->getTargetTask();
    EXPECT_EQ(3, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("d3", 2)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("d5", 2)) != taskSet.end());

    taskSet = brokers[ro0]->getTargetTask();
    EXPECT_EQ(2, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("o2", 0)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("o3", 0)) != taskSet.end());
    taskSet = brokers[ro1]->getTargetTask();
    EXPECT_EQ(2, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("o2", 1)) != taskSet.end());
    EXPECT_TRUE(taskSet.find(make_pair("o3", 1)) != taskSet.end());
    taskSet = brokers[ro2]->getTargetTask();
    EXPECT_EQ(1, taskSet.size());
    EXPECT_TRUE(taskSet.find(make_pair("o3", 2)) != taskSet.end());
}

TEST_F(BrokerDecisionMakerTest, testVeticalBrokerDead) {
    PartitionPairList partitionPairs;
    vector<TopicInfoPtr> tinfos; // 10 topics, partCnt 1->10
    for (int i = 1; i <= 10; i++) {
        tinfos.emplace_back(createTopicInfo("d" + to_string(i), i, 1, i > 7 ? 2 : 1, "default"));
    }
    for (auto &topic : tinfos) {
        for (int pid = 0; pid < topic->getPartitionCount(); ++pid) {
            partitionPairs.push_back(make_pair(topic, pid));
        }
    }

    // case 1. only one broker
    string role = "default##broker_2_0";
    string address = role + "#_#10.10.10.10:1002";
    WorkerMap brokers;
    brokers[role] = constructBroker(address, "nt", 0);
    map<string, uint32_t> veticalGroupBrokerCnt;
    veticalGroupBrokerCnt["default"] = 5;
    BrokerDecisionMaker decisionMaker("vetical", veticalGroupBrokerCnt);
    auto partPairs = partitionPairs;
    decisionMaker.vetical(partPairs, brokers);
    for (auto &topic : tinfos) {
        const string &topicName = topic->getTopicName();
        for (int pid = 0; pid < topic->getPartitionCount(); ++pid) {
            if ((2 == pid) || (7 == pid && ("d8" == topicName || "d9" == topicName || "d10" == topicName)) ||
                (0 == pid && ("d1" == topicName || "d2" == topicName))) {
                EXPECT_EQ(address, topic->getTargetRoleAddress(pid));
            } else {
                EXPECT_EQ(string(""), topic->getTargetRoleAddress(pid));
            }
        }
    }

    auto curTime = autil::TimeUtility::currentTime();
    for (auto &broker : brokers) {
        broker.second->prepareDecision(curTime, curTime);
    }

    // case 2. have 8 brokers, but 3 of them are dead, broker_0, broker_3, broker_6 dead
    vector<string> brokerAddrs;
    for (int i = 0; i < 8; ++i) {
        role = "default##broker_" + to_string(i) + "_0";
        address = role + "#_#10.10.10.10:100" + to_string(i);
        brokerAddrs.emplace_back(address);
        if (0 == i || 3 == i || 6 == i) {
            continue;
        }
        brokers[role] = constructBroker(address, "nt", 0);
    }

    partPairs = partitionPairs;
    decisionMaker.vetical(partPairs, brokers);
    for (int i = 0; i < 5; ++i) { // topic 1->5, limit 1
        TopicInfoPtr tinfo = tinfos[i];
        EXPECT_EQ(brokerAddrs[7], tinfo->getTargetRoleAddress(0));
        if (i > 0) {
            EXPECT_EQ(brokerAddrs[1], tinfo->getTargetRoleAddress(1));
        }
        if (i > 1) {
            EXPECT_EQ(brokerAddrs[2], tinfo->getTargetRoleAddress(2));
        }
        if (i > 2) {
            EXPECT_EQ(brokerAddrs[5], tinfo->getTargetRoleAddress(3));
        }
        if (i > 3) {
            EXPECT_EQ(brokerAddrs[4], tinfo->getTargetRoleAddress(4));
        }
        if (i > 4) {
            EXPECT_EQ(brokerAddrs[5], tinfo->getTargetRoleAddress(5));
        }
    }
    for (int i = 5; i < 7; ++i) { // topic 6 7, limit 1
        TopicInfoPtr tinfo = tinfos[i];
        EXPECT_EQ(brokerAddrs[7], tinfo->getTargetRoleAddress(0));
        EXPECT_EQ(brokerAddrs[1], tinfo->getTargetRoleAddress(1));
        EXPECT_EQ(brokerAddrs[2], tinfo->getTargetRoleAddress(2));
        EXPECT_EQ(brokerAddrs[5], tinfo->getTargetRoleAddress(3));
        EXPECT_EQ(brokerAddrs[4], tinfo->getTargetRoleAddress(4));
        EXPECT_EQ("", tinfo->getTargetRoleAddress(5));
        if (i == 6) {
            EXPECT_EQ("", tinfo->getTargetRoleAddress(6));
        }
    }

    TopicInfoPtr tinfo = tinfos[7]; // topic 8, limit 2
    EXPECT_EQ(brokerAddrs[7], tinfo->getTargetRoleAddress(0));
    EXPECT_EQ(brokerAddrs[1], tinfo->getTargetRoleAddress(1));
    EXPECT_EQ(brokerAddrs[2], tinfo->getTargetRoleAddress(2));
    EXPECT_EQ(brokerAddrs[7], tinfo->getTargetRoleAddress(3));
    EXPECT_EQ(brokerAddrs[4], tinfo->getTargetRoleAddress(4));
    EXPECT_EQ(brokerAddrs[5], tinfo->getTargetRoleAddress(5));
    EXPECT_EQ(brokerAddrs[1], tinfo->getTargetRoleAddress(6));
    EXPECT_EQ(brokerAddrs[2], tinfo->getTargetRoleAddress(7));

    for (int i = 8; i < 10; ++i) { // topic 9 10, limit 2
        TopicInfoPtr tinfo = tinfos[i];
        EXPECT_EQ(brokerAddrs[7], tinfo->getTargetRoleAddress(0));
        EXPECT_EQ(brokerAddrs[1], tinfo->getTargetRoleAddress(1));
        EXPECT_EQ(brokerAddrs[2], tinfo->getTargetRoleAddress(2));
        EXPECT_EQ(brokerAddrs[7], tinfo->getTargetRoleAddress(3));
        EXPECT_EQ(brokerAddrs[4], tinfo->getTargetRoleAddress(4));
        EXPECT_EQ(brokerAddrs[5], tinfo->getTargetRoleAddress(5));
        EXPECT_EQ(brokerAddrs[1], tinfo->getTargetRoleAddress(6));
        EXPECT_EQ(brokerAddrs[2], tinfo->getTargetRoleAddress(7));
        EXPECT_EQ(brokerAddrs[5], tinfo->getTargetRoleAddress(8));
        if (i == 9) {
            EXPECT_EQ(brokerAddrs[4], tinfo->getTargetRoleAddress(9));
        }
    }

    // case 3. have 8 brokers, broker_0, broker_3, broker_6 alive again
    for (int i = 0; i < 8; ++i) {
        if (0 == i || 3 == i || 6 == i) {
            role = "default##broker_" + to_string(i) + "_0";
            address = role + "#_#10.10.10.10:100" + to_string(i);
            brokers[role] = constructBroker(address, "nt", 0);
        }
    }

    for (auto &broker : brokers) {
        broker.second->prepareDecision(curTime, curTime);
    }
    partPairs = partitionPairs;
    decisionMaker.vetical(partPairs, brokers);
    for (int i = 0; i < 10; ++i) { // topic 1->5, limit 1
        TopicInfoPtr tinfo = tinfos[i];
        EXPECT_EQ(brokerAddrs[0], tinfo->getTargetRoleAddress(0));
        if (i > 0) {
            EXPECT_EQ(brokerAddrs[1], tinfo->getTargetRoleAddress(1));
        }
        if (i > 1) {
            EXPECT_EQ(brokerAddrs[2], tinfo->getTargetRoleAddress(2));
        }
        if (i > 2) {
            EXPECT_EQ(brokerAddrs[3], tinfo->getTargetRoleAddress(3));
        }
        if (i > 3) {
            EXPECT_EQ(brokerAddrs[4], tinfo->getTargetRoleAddress(4));
        }
    }
    tinfo = tinfos[5]; // topic 6, 7 limit 1
    EXPECT_EQ(brokerAddrs[7], tinfo->getTargetRoleAddress(5));
    tinfo = tinfos[6];
    EXPECT_EQ(brokerAddrs[7], tinfo->getTargetRoleAddress(5));
    EXPECT_EQ(brokerAddrs[6], tinfo->getTargetRoleAddress(6));
    for (int i = 7; i < 10; ++i) { // topic 8 9 10, limit 2
        TopicInfoPtr tinfo = tinfos[i];
        EXPECT_EQ(brokerAddrs[0], tinfo->getTargetRoleAddress(5));
        EXPECT_EQ(brokerAddrs[1], tinfo->getTargetRoleAddress(6));
        EXPECT_EQ(brokerAddrs[2], tinfo->getTargetRoleAddress(7));
        if (i > 7) {
            EXPECT_EQ(brokerAddrs[3], tinfo->getTargetRoleAddress(8));
        }
        if (i == 9) {
            EXPECT_EQ(brokerAddrs[4], tinfo->getTargetRoleAddress(9));
        }
    }
}

} // namespace admin
} // namespace swift
