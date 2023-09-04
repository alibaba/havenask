#include "swift/admin/TopicTable.h"

#include <cstddef>
#include <iostream>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/admin/TopicInfo.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace fslib;
using namespace fslib::fs;

namespace swift {
namespace admin {

class TopicTableTest : public TESTBASE {
public:
    TopicTableTest();
    ~TopicTableTest();

public:
    void setUp();
    void tearDown();

protected:
    void prepareTopicInfo(protocol::TopicCreationRequest &request,
                          std::string topicName,
                          uint32_t partitionCount,
                          uint32_t minBufferSize = 8,
                          uint32_t maxBufferSize = 128,
                          uint32_t resource = 100,
                          uint32_t partitionLimit = 100000,
                          protocol::TopicMode topicMode = protocol::TOPIC_MODE_NORMAL,
                          bool needFieldFilter = false,
                          int64_t obsoleteFileTimeInterval = -1,
                          int32_t reservedFileCount = -1);

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, TopicTableTest);

using namespace std;
using namespace swift::protocol;

TopicTableTest::TopicTableTest() {}

TopicTableTest::~TopicTableTest() {}

void TopicTableTest::setUp() { AUTIL_LOG(DEBUG, "setUp!"); }

void TopicTableTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

void TopicTableTest::prepareTopicInfo(protocol::TopicCreationRequest &request,
                                      string topicName,
                                      uint32_t partitionCount,
                                      uint32_t minBufferSize,
                                      uint32_t maxBufferSize,
                                      uint32_t resource,
                                      uint32_t partitionLimit,
                                      protocol::TopicMode topicMode,
                                      bool needFieldFilter,
                                      int64_t obsoleteFileTimeInterval,
                                      int32_t reservedFileCount) {
    request.Clear();
    request.set_topicname(topicName);
    request.set_partitioncount(partitionCount);
    request.set_partitionminbuffersize(minBufferSize);
    request.set_partitionmaxbuffersize(maxBufferSize);
    request.set_resource(resource);
    request.set_partitionlimit(partitionLimit);
    request.set_topicmode(topicMode);
    request.set_needfieldfilter(needFieldFilter);
    request.set_obsoletefiletimeinterval(obsoleteFileTimeInterval);
    request.set_reservedfilecount(reservedFileCount);
}

TEST_F(TopicTableTest, testAddTopic) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    TopicTable topicTable;
    protocol::TopicCreationRequest request;
    prepareTopicInfo(request, "t1", 10);
    ASSERT_TRUE(topicTable.addTopic(&request));
    prepareTopicInfo(request, "t2", 10);
    ASSERT_TRUE(topicTable.addTopic(&request));
    prepareTopicInfo(request, "t2", 20);
    ASSERT_TRUE(!topicTable.addTopic(&request));
    prepareTopicInfo(request, "t3", 10, 10, 50, 500, 5, TOPIC_MODE_SECURITY, true, 100, 5);
    ASSERT_TRUE(topicTable.addTopic(&request));
    prepareTopicInfo(request, "t4", 2);
    request.set_topictype(TOPIC_TYPE_LOGIC);
    ASSERT_TRUE(topicTable.addTopic(&request));

    TopicMap topicMap;
    topicTable.getTopicMap(topicMap);
    ASSERT_EQ((size_t)4, topicMap.size());
    ASSERT_TRUE(topicMap.find("t1") != topicMap.end());
    ASSERT_TRUE(topicMap.find("t2") != topicMap.end());
    ASSERT_TRUE(topicMap.find("t3") != topicMap.end());
    ASSERT_TRUE(topicMap.find("t4") != topicMap.end());
    ASSERT_EQ(TOPIC_TYPE_NORMAL, topicMap["t1"]->getTopicType());
    ASSERT_EQ(TOPIC_MODE_NORMAL, topicMap.find("t1")->second->getTopicMeta().topicmode());
    ASSERT_TRUE(!topicMap.find("t1")->second->getTopicMeta().needfieldfilter());
    ASSERT_EQ((int64_t)-1, topicMap.find("t1")->second->getTopicMeta().obsoletefiletimeinterval());
    ASSERT_EQ((int32_t)-1, topicMap.find("t1")->second->getTopicMeta().reservedfilecount());
    ASSERT_EQ((uint32_t)10, topicMap.find("t3")->second->getTopicMeta().partitioncount());
    ASSERT_EQ((uint32_t)5, topicMap.find("t3")->second->getTopicMeta().partitionlimit());
    ASSERT_EQ(TOPIC_TYPE_NORMAL, topicMap["t3"]->getTopicType());
    ASSERT_EQ(TOPIC_MODE_SECURITY, topicMap.find("t3")->second->getTopicMeta().topicmode());
    ASSERT_TRUE(topicMap.find("t3")->second->getTopicMeta().needfieldfilter());
    ASSERT_EQ((int64_t)100, topicMap.find("t3")->second->getTopicMeta().obsoletefiletimeinterval());
    ASSERT_EQ((int32_t)5, topicMap.find("t3")->second->getTopicMeta().reservedfilecount());
    ASSERT_EQ(TOPIC_TYPE_LOGIC, topicMap["t4"]->getTopicType());
    ASSERT_EQ(PARTITION_STATUS_WAITING, topicMap["t1"]->getStatus(0));
    ASSERT_EQ(PARTITION_STATUS_WAITING, topicMap["t2"]->getStatus(0));
    ASSERT_EQ(PARTITION_STATUS_WAITING, topicMap["t3"]->getStatus(0));
    ASSERT_EQ(PARTITION_STATUS_RUNNING, topicMap["t4"]->getStatus(0));
}

TEST_F(TopicTableTest, testUpdateTopic) {
    TopicTable topicTable;
    protocol::TopicCreationRequest request;
    prepareTopicInfo(request, "t1", 10);
    ASSERT_TRUE(topicTable.addTopic(&request));
    prepareTopicInfo(request, "t2", 1);
    request.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    ASSERT_TRUE(topicTable.addTopic(&request));

    TopicMap topicMap;
    topicTable.getTopicMap(topicMap);
    ASSERT_EQ((size_t)2, topicMap.size());
    ASSERT_TRUE(topicMap.find("t1") != topicMap.end());
    ASSERT_TRUE(topicMap.find("t2") != topicMap.end());

    topicTable.getTopicMap(topicMap);
    ASSERT_EQ(TOPIC_TYPE_NORMAL, topicMap["t1"]->getTopicType());
    ASSERT_EQ(PARTITION_STATUS_WAITING, topicMap["t1"]->getStatus(0));
    ASSERT_EQ(10, topicMap["t1"]->_topicMeta.partitioncount());
    ASSERT_FALSE(topicMap["t1"]->_topicMeta.sealed());
    ASSERT_EQ(-1, topicMap["t1"]->_topicMeta.topicexpiredtime());

    ASSERT_EQ(TOPIC_TYPE_LOGIC_PHYSIC, topicMap["t2"]->getTopicType());
    ASSERT_EQ(PARTITION_STATUS_WAITING, topicMap["t2"]->getStatus(0));
    ASSERT_EQ(1, topicMap["t2"]->_topicMeta.partitioncount());
    ASSERT_FALSE(topicMap["t2"]->_topicMeta.sealed());
    ASSERT_EQ(-1, topicMap["t2"]->_topicMeta.topicexpiredtime());

    // update success
    TopicCreationRequest modifyT1;
    modifyT1.set_topicname("t1");
    modifyT1.set_partitioncount(10);
    modifyT1.set_topicexpiredtime(100);
    modifyT1.set_sealed(true);
    ASSERT_TRUE(topicTable.updateTopic(&modifyT1)); // update t1
    topicTable.getTopicMap(topicMap);
    ASSERT_EQ(TOPIC_TYPE_NORMAL, topicMap["t1"]->getTopicType());
    ASSERT_EQ(PARTITION_STATUS_WAITING, topicMap["t1"]->getStatus(0));
    ASSERT_EQ(10, topicMap["t1"]->_topicMeta.partitioncount());
    ASSERT_TRUE(topicMap["t1"]->_topicMeta.sealed());
    ASSERT_EQ(100, topicMap["t1"]->_topicMeta.topicexpiredtime());
    // t2 not change
    ASSERT_EQ(TOPIC_TYPE_LOGIC_PHYSIC, topicMap["t2"]->getTopicType());
    ASSERT_EQ(PARTITION_STATUS_WAITING, topicMap["t2"]->getStatus(0));
    ASSERT_EQ(1, topicMap["t2"]->_topicMeta.partitioncount());
    ASSERT_FALSE(topicMap["t2"]->_topicMeta.sealed());
    ASSERT_EQ(-1, topicMap["t2"]->_topicMeta.topicexpiredtime());

    // update t2
    TopicCreationRequest modifyT2;
    modifyT2.set_topicname("t2");
    modifyT2.set_partitioncount(10);
    modifyT2.set_topicexpiredtime(100);
    modifyT2.set_sealed(true);
    modifyT2.set_topictype(TOPIC_TYPE_LOGIC);
    ASSERT_TRUE(topicTable.updateTopic(&modifyT2));
    topicTable.getTopicMap(topicMap);
    ASSERT_EQ(TOPIC_TYPE_LOGIC, topicMap["t2"]->getTopicType());
    ASSERT_EQ(PARTITION_STATUS_RUNNING, topicMap["t2"]->getStatus(0));
    ASSERT_EQ(10, topicMap["t2"]->_topicMeta.partitioncount());
    ASSERT_TRUE(topicMap["t2"]->_topicMeta.sealed());
    ASSERT_EQ(100, topicMap["t2"]->_topicMeta.topicexpiredtime());

    // update failed
    request.set_topicname("t3");
    ASSERT_FALSE(topicTable.updateTopic(&request));
}

TEST_F(TopicTableTest, testDelTopic) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    string dataRoot = GET_TEMPLATE_DATA_PATH();
    cout << "dataRoot: " << dataRoot << endl;
    TopicTable topicTable;
    protocol::TopicCreationRequest request;
    prepareTopicInfo(request, "t1", 10);
    request.set_dfsroot(dataRoot);
    ASSERT_TRUE(topicTable.addTopic(&request));
    FileSystem::mkDir(dataRoot + "/t1");
    ASSERT_EQ(EC_TRUE, FileSystem::isExist(dataRoot + "/t1"));

    prepareTopicInfo(request, "t2", 10);
    request.set_dfsroot(dataRoot);
    ASSERT_TRUE(topicTable.addTopic(&request));
    FileSystem::mkDir(dataRoot + "/t2");
    ASSERT_EQ(EC_TRUE, FileSystem::isExist(dataRoot + "/t2"));

    ASSERT_TRUE(topicTable.delTopic("t1"));
    ASSERT_EQ(EC_TRUE, FileSystem::isExist(dataRoot + "/t1"));
    ASSERT_TRUE(!topicTable.delTopic("t3"));
    TopicMap topicMap;
    topicTable.getTopicMap(topicMap);
    ASSERT_EQ((size_t)1, topicMap.size());
    ASSERT_TRUE(topicMap.find("t2") != topicMap.end());
    ASSERT_TRUE(topicTable.delTopic("t2", true));
    ASSERT_EQ(EC_FALSE, FileSystem::isExist(dataRoot + "/t2"));
    ASSERT_EQ(EC_TRUE, FileSystem::isExist(dataRoot + "/t1"));
}

TEST_F(TopicTableTest, testFindTopic) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    TopicTable topicTable;
    protocol::TopicCreationRequest request;
    prepareTopicInfo(request, "t1", 10);
    ASSERT_TRUE(topicTable.addTopic(&request));
    prepareTopicInfo(request, "t2", 10);
    ASSERT_TRUE(topicTable.addTopic(&request));
    TopicInfoPtr topicInfoPtr = topicTable.findTopic("t1");
    ASSERT_TRUE(topicInfoPtr != NULL);
    topicInfoPtr = topicTable.findTopic("t3");
    ASSERT_TRUE(topicInfoPtr == NULL);
}

TEST_F(TopicTableTest, testPrepareDecision) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    TopicTable topicTable;
    string topicName1 = "news_search";
    string topicName2 = "news_search1";
    protocol::TopicCreationRequest request;
    prepareTopicInfo(request, topicName1, 2);
    ASSERT_TRUE(topicTable.addTopic(&request));
    prepareTopicInfo(request, topicName2, 1);
    ASSERT_TRUE(topicTable.addTopic(&request));

    TopicMap topicMap;
    topicTable.getTopicMap(topicMap);
    topicMap[topicName1]->setTargetRoleAddress(0, "role_1#_#123.123.123.123:123");
    topicMap[topicName2]->setTargetRoleAddress(0, "role_1#_#123.123.123.123:123");

    topicTable.prepareDecision();

    ASSERT_EQ(string(""), topicMap[topicName1]->getTargetRoleAddress(0));
    ASSERT_EQ(string(""), topicMap[topicName1]->getTargetRoleAddress(1));
    ASSERT_EQ(string(""), topicMap[topicName2]->getTargetRoleAddress(0));
}

TEST_F(TopicTableTest, testSplitTopicMapBySchedule) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    TopicTable topicTable;
    protocol::TopicCreationRequest request;
    prepareTopicInfo(request, "t1", 2);
    request.set_topictype(TOPIC_TYPE_LOGIC);
    ASSERT_TRUE(topicTable.addTopic(&request));
    prepareTopicInfo(request, "t2", 1);
    request.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    ASSERT_TRUE(topicTable.addTopic(&request));
    prepareTopicInfo(request, "t3", 3);
    request.set_topictype(TOPIC_TYPE_PHYSIC);
    ASSERT_TRUE(topicTable.addTopic(&request));
    prepareTopicInfo(request, "t4", 4);
    request.set_topictype(TOPIC_TYPE_NORMAL);
    ASSERT_TRUE(topicTable.addTopic(&request));

    TopicMap topicMap;
    TopicMap logicTopicMap;
    topicTable.splitTopicMapBySchedule(topicMap, logicTopicMap);
    ASSERT_EQ(3, topicMap.size());
    ASSERT_TRUE(topicMap.find("t1") == topicMap.end());
    ASSERT_TRUE(topicMap.find("t2") != topicMap.end());
    ASSERT_TRUE(topicMap.find("t3") != topicMap.end());
    ASSERT_TRUE(topicMap.find("t4") != topicMap.end());

    ASSERT_EQ(1, logicTopicMap.size());
    ASSERT_TRUE(logicTopicMap.find("t1") != logicTopicMap.end());
    ASSERT_TRUE(logicTopicMap.find("t2") == logicTopicMap.end());
    ASSERT_TRUE(logicTopicMap.find("t3") == logicTopicMap.end());
    ASSERT_TRUE(logicTopicMap.find("t4") == logicTopicMap.end());
}

TEST_F(TopicTableTest, testGetNeedMergeTopicMap) {
    TopicMap topicMap;
    {
        TopicTable topicTable;
        topicTable.getNeedMergeTopicMap(topicMap);
        EXPECT_TRUE(topicMap.empty());
    }
    {
        // only disable
        TopicTable topicTable;
        TopicCreationRequest topicMeta;
        topicMeta.set_topicname("1");
        topicMeta.set_partitioncount(1);
        topicMeta.set_enablemergedata(false);
        topicTable.addTopic(&topicMeta);
        topicMeta.set_topicname("2");
        topicTable.addTopic(&topicMeta);
        topicMeta.set_topicname("3");
        topicTable.addTopic(&topicMeta);

        topicTable.getNeedMergeTopicMap(topicMap);
        EXPECT_TRUE(topicMap.empty());
    }
    {
        TopicTable topicTable;
        TopicCreationRequest topicMeta;
        topicMeta.set_topicname("1");
        topicMeta.set_partitioncount(1);
        topicMeta.set_enablemergedata(false);
        topicTable.addTopic(&topicMeta);

        topicMeta.set_topicname("2");
        topicMeta.set_enablemergedata(true);
        topicTable.addTopic(&topicMeta);

        topicMeta.set_topicname("3");
        topicMeta.set_enablemergedata(false);
        topicTable.addTopic(&topicMeta);

        topicMeta.set_topicname("4");
        topicTable.addTopic(&topicMeta);

        topicTable.getNeedMergeTopicMap(topicMap);
        EXPECT_EQ(1, topicMap.size());
    }
}

} // namespace admin
} // namespace swift
