#include "swift/admin/CleanAtDeleteManager.h"

#include <iostream>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/ExceptionTrigger.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/common/Common.h"
#include "swift/common/PathDefine.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::protocol;
using namespace swift::common;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;

namespace swift {
namespace admin {

class CleanAtDeleteManagerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    cm_basic::ZkWrapper *_zkWrapper = nullptr;
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
};

void CleanAtDeleteManagerTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    string zkPath = "zfs://" + oss.str() + "/";
    _zkWrapper = new cm_basic::ZkWrapper(oss.str(), 1000);
    ASSERT_TRUE(_zkWrapper->open());
    _zkRoot = zkPath + GET_CLASS_NAME();
    cout << "zkRoot: " << _zkRoot << endl;
    fs::FileSystem::mkDir(_zkRoot);
}

void CleanAtDeleteManagerTest::tearDown() { DELETE_AND_SET_NULL(_zkWrapper); }

TEST_F(CleanAtDeleteManagerTest, testInit) {
    AdminZkDataAccessorPtr zkAccessor = AdminZkDataAccessorPtr(new AdminZkDataAccessor());
    zkAccessor->init(_zkWrapper, _zkRoot);
    CleanAtDeleteTopicTasks tasks;
    CleanAtDeleteTopicTask *task = tasks.add_tasks();
    task->set_topicname("_full_processed__topic1");
    task = tasks.add_tasks();
    task->set_topicname("_full_processed_topic2");
    task = tasks.add_tasks();
    task->set_topicname("foo_irregular_feature_");
    zkAccessor->serializeCleanAtDeleteTasks(tasks);
    CleanAtDeleteManager manager;
    vector<string> patterns{"_full_processed_", "_irregular_feature_"};
    manager.init(patterns, zkAccessor, 1234);
    EXPECT_EQ(1234, manager._cleanDataIntervalSec);
    EXPECT_EQ(2, manager._patterns.size());
    EXPECT_EQ("_full_processed_", manager._patterns[0]);
    EXPECT_EQ("_irregular_feature_", manager._patterns[1]);
    EXPECT_EQ(3, manager._cleanAtDeleteTasks.tasks_size());
    EXPECT_EQ("_full_processed__topic1", manager._cleanAtDeleteTasks.tasks(0).topicname());
    EXPECT_EQ("_full_processed_topic2", manager._cleanAtDeleteTasks.tasks(1).topicname());
    EXPECT_EQ("foo_irregular_feature_", manager._cleanAtDeleteTasks.tasks(2).topicname());
    EXPECT_EQ(3, manager._cleaningTopics.size());
    EXPECT_EQ(1, manager._cleaningTopics.count("_full_processed__topic1"));
    EXPECT_EQ(1, manager._cleaningTopics.count("_full_processed_topic2"));
    EXPECT_EQ(1, manager._cleaningTopics.count("foo_irregular_feature_"));
}

TEST_F(CleanAtDeleteManagerTest, testExistCleanAtDeleteTopic) {
    CleanAtDeleteManager manager;
    EXPECT_EQ(0, manager._cleanAtDeleteTasks.tasks_size());
    EXPECT_FALSE(manager.existCleanAtDeleteTopic("calsual"));
    CleanAtDeleteTopicTask *task = manager._cleanAtDeleteTasks.add_tasks();
    task->set_topicname("trest1");
    EXPECT_FALSE(manager.existCleanAtDeleteTopic("calsual"));
    EXPECT_FALSE(manager.existCleanAtDeleteTopic("trest"));
    EXPECT_TRUE(manager.existCleanAtDeleteTopic("trest1"));
    EXPECT_FALSE(manager.existCleanAtDeleteTopic(""));
}

TEST_F(CleanAtDeleteManagerTest, testNeedCleanDataAtOnce) {
    vector<string> patterns;
    patterns.push_back("pattern1");
    patterns.push_back("_pattern_2_");
    CleanAtDeleteManager manager;
    manager._patterns = patterns;

    EXPECT_TRUE(manager.needCleanDataAtOnce("pattern1_topic"));
    CleanAtDeleteTopicTask *task = manager._cleanAtDeleteTasks.add_tasks();
    task->set_topicname("pattern1_topic");
    EXPECT_FALSE(manager.needCleanDataAtOnce("pattern1_topic"));
    EXPECT_FALSE(manager.needCleanDataAtOnce("pattern2_topic"));
    EXPECT_TRUE(manager.needCleanDataAtOnce("_pattern_2_topic"));
    EXPECT_FALSE(manager.needCleanDataAtOnce("none_feature_plain_topic"));
    EXPECT_FALSE(manager.needCleanDataAtOnce(""));
}

TEST_F(CleanAtDeleteManagerTest, testPushCleanAtDeleteTopic) {
    CleanAtDeleteManager manager;
    manager._dataAccessor.reset(new AdminZkDataAccessor());
    manager._dataAccessor->init(_zkWrapper, _zkRoot);
    EXPECT_EQ(0, manager._cleanAtDeleteTasks.tasks_size());

    uint32_t startTime = TimeUtility::currentTimeInSeconds();
    TopicCreationRequest request;
    request.set_topicname("cad_topic1");
    request.set_dfsroot("dfs://na71/secret");
    manager.pushCleanAtDeleteTopic("cad_topic1", request);
    EXPECT_EQ(1, manager._cleaningTopics.size());
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic1"));
    EXPECT_EQ(0, manager._cleaningTopics.count("cad_topic3"));
    EXPECT_EQ(1, manager._cleanAtDeleteTasks.tasks_size());
    EXPECT_EQ("cad_topic1", manager._cleanAtDeleteTasks.tasks(0).topicname());
    EXPECT_GE(manager._cleanAtDeleteTasks.tasks(0).deletetimestampsec(), startTime);
    uint32_t taskTime1 = manager._cleanAtDeleteTasks.tasks(0).deletetimestampsec();
    EXPECT_EQ(1, manager._cleanAtDeleteTasks.tasks(0).dfspath_size());
    EXPECT_EQ("dfs://na71/secret/cad_topic1", manager._cleanAtDeleteTasks.tasks(0).dfspath(0));

    startTime = TimeUtility::currentTimeInSeconds();
    request.set_topicname("cad_topic2");
    *(request.add_extenddfsroot()) = "dfs://na81/extend/1";
    *(request.add_extenddfsroot()) = "dfs://na82/part2/0";
    manager.pushCleanAtDeleteTopic("cad_topic2", request);
    EXPECT_EQ(2, manager._cleaningTopics.size());
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic1"));
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic2"));
    EXPECT_EQ(2, manager._cleanAtDeleteTasks.tasks_size());
    EXPECT_EQ("cad_topic2", manager._cleanAtDeleteTasks.tasks(1).topicname());
    EXPECT_GE(manager._cleanAtDeleteTasks.tasks(1).deletetimestampsec(), startTime);
    uint32_t taskTime2 = manager._cleanAtDeleteTasks.tasks(1).deletetimestampsec();
    EXPECT_EQ(3, manager._cleanAtDeleteTasks.tasks(1).dfspath_size());
    EXPECT_EQ("dfs://na71/secret/cad_topic2", manager._cleanAtDeleteTasks.tasks(1).dfspath(0));
    EXPECT_EQ("dfs://na81/extend/1/cad_topic2", manager._cleanAtDeleteTasks.tasks(1).dfspath(1));
    EXPECT_EQ("dfs://na82/part2/0/cad_topic2", manager._cleanAtDeleteTasks.tasks(1).dfspath(2));

    request.set_topicname("cad_topic3");
    request.clear_dfsroot();
    request.clear_extenddfsroot();
    manager.pushCleanAtDeleteTopic("cad_topic3", request);
    EXPECT_EQ(2, manager._cleanAtDeleteTasks.tasks_size());

    const string &path =
        StringUtil::formatString("%s/clean_at_delete_tasks", PathDefine::getPathFromZkPath(_zkRoot).c_str());
    CleanAtDeleteTopicTasks remoteTasks;
    EXPECT_TRUE(manager._dataAccessor->readZk(path, remoteTasks, true, false));
    EXPECT_EQ(2, remoteTasks.tasks_size());
    EXPECT_EQ("cad_topic1", remoteTasks.tasks(0).topicname());
    EXPECT_EQ(taskTime1, manager._cleanAtDeleteTasks.tasks(0).deletetimestampsec());
    EXPECT_EQ(1, remoteTasks.tasks(0).dfspath_size());
    EXPECT_EQ("dfs://na71/secret/cad_topic1", remoteTasks.tasks(0).dfspath(0));
    EXPECT_EQ("cad_topic2", remoteTasks.tasks(1).topicname());
    EXPECT_EQ(taskTime2, manager._cleanAtDeleteTasks.tasks(1).deletetimestampsec());
    EXPECT_EQ(3, remoteTasks.tasks(1).dfspath_size());
    EXPECT_EQ("dfs://na71/secret/cad_topic2", remoteTasks.tasks(1).dfspath(0));
    EXPECT_EQ("dfs://na81/extend/1/cad_topic2", remoteTasks.tasks(1).dfspath(1));
    EXPECT_EQ("dfs://na82/part2/0/cad_topic2", remoteTasks.tasks(1).dfspath(2));

    CleanAtDeleteTopicTasks emptyTasks;
    manager._dataAccessor->serializeCleanAtDeleteTasks(emptyTasks);
}

TEST_F(CleanAtDeleteManagerTest, testRemoveCleanAtDeleteTopicData) {
    CleanAtDeleteManager manager;
    manager._dataAccessor.reset(new AdminZkDataAccessor());
    manager._dataAccessor->init(_zkWrapper, _zkRoot);
    EXPECT_EQ(0, manager._cleanAtDeleteTasks.tasks_size());
    unordered_set<string> topicSet;
    topicSet.insert("cad_topic1");
    topicSet.insert("cad_topic2");
    topicSet.insert("cad_topic3");
    manager.removeCleanAtDeleteTopicData(topicSet); // test no coredump

    uint32_t startTime = TimeUtility::currentTimeInSeconds();
    vector<string> cadTopic0Path = {"mock://path0/cad_topic0", "mock://path2/cad_topic0"};
    manager.doPushCleanAtDeleteTopic("cad_topic0", cadTopic0Path);
    EXPECT_EQ(3600, manager._cleanDataIntervalSec);
    manager.removeCleanAtDeleteTopicData(topicSet);
    EXPECT_EQ(1, manager._cleaningTopics.size());
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic0"));
    EXPECT_EQ(1, manager._cleanAtDeleteTasks.tasks_size());
    EXPECT_EQ("cad_topic0", manager._cleanAtDeleteTasks.tasks(0).topicname());
    EXPECT_GE(manager._cleanAtDeleteTasks.tasks(0).deletetimestampsec(), startTime);
    EXPECT_EQ(2, manager._cleanAtDeleteTasks.tasks(0).dfspath_size());
    EXPECT_EQ("mock://path0/cad_topic0", manager._cleanAtDeleteTasks.tasks(0).dfspath(0));
    EXPECT_EQ("mock://path2/cad_topic0", manager._cleanAtDeleteTasks.tasks(0).dfspath(1));
    CleanAtDeleteTopicTasks emptyTasks;
    manager._cleanAtDeleteTasks = emptyTasks;
    manager._dataAccessor->serializeCleanAtDeleteTasks(emptyTasks);

    manager._cleanDataIntervalSec = 0;
    vector<string> cadTopic1Path = {"mock://path1/cad_topic1", "mock://path2/cad_topic1"};
    manager.doPushCleanAtDeleteTopic("cad_topic1", cadTopic1Path);
    vector<string> cadTopic2Path = {
        "mock://ea117/path1/cad_topic2", "mock://ea118/path0/cad_topic2", "mock://ea119/path2/cad_topic2"};
    manager.doPushCleanAtDeleteTopic("cad_topic2", cadTopic2Path);
    manager.removeCleanAtDeleteTopicData(topicSet);
    EXPECT_EQ(2, manager._cleaningTopics.size());
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic1"));
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic2"));
    EXPECT_EQ(0, manager._cleaningTopics.count("cad_topic3"));
    EXPECT_EQ(2, manager._cleanAtDeleteTasks.tasks_size());
    EXPECT_EQ("cad_topic1", manager._cleanAtDeleteTasks.tasks(0).topicname());
    EXPECT_EQ(2, manager._cleanAtDeleteTasks.tasks(0).dfspath_size());
    EXPECT_EQ("mock://path1/cad_topic1", manager._cleanAtDeleteTasks.tasks(0).dfspath(0));
    EXPECT_EQ("mock://path2/cad_topic1", manager._cleanAtDeleteTasks.tasks(0).dfspath(1));
    EXPECT_EQ("cad_topic2", manager._cleanAtDeleteTasks.tasks(1).topicname());
    EXPECT_EQ(3, manager._cleanAtDeleteTasks.tasks(1).dfspath_size());
    EXPECT_EQ("mock://ea117/path1/cad_topic2", manager._cleanAtDeleteTasks.tasks(1).dfspath(0));
    EXPECT_EQ("mock://ea118/path0/cad_topic2", manager._cleanAtDeleteTasks.tasks(1).dfspath(1));
    EXPECT_EQ("mock://ea119/path2/cad_topic2", manager._cleanAtDeleteTasks.tasks(1).dfspath(2));
    manager._cleanAtDeleteTasks = emptyTasks;
    manager._dataAccessor->serializeCleanAtDeleteTasks(emptyTasks);

    manager.doPushCleanAtDeleteTopic("cad_topic2", cadTopic2Path);
    manager.doPushCleanAtDeleteTopic("cad_topic1", cadTopic1Path);
    EXPECT_EQ(2, manager._cleaningTopics.size());
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic1"));
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic2"));
    EXPECT_EQ(0, manager._cleaningTopics.count("cad_topic3"));
    FileSystem::_useMock = true;
    ExceptionTrigger::InitTrigger(0, true, false);
    ExceptionTrigger::SetExceptionCondition("remove", "mock://path1/cad_topic1", fslib::EC_OK);
    ExceptionTrigger::SetExceptionCondition("remove", "mock://path2/cad_topic1", fslib::EC_OK);
    topicSet.erase("cad_topic1");
    manager.removeCleanAtDeleteTopicData(topicSet);
    EXPECT_EQ(1, manager._cleaningTopics.size());
    EXPECT_EQ(0, manager._cleaningTopics.count("cad_topic1"));
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic2"));
    EXPECT_EQ(0, manager._cleaningTopics.count("cad_topic3"));
    EXPECT_EQ(1, manager._cleanAtDeleteTasks.tasks_size());
    EXPECT_EQ("cad_topic2", manager._cleanAtDeleteTasks.tasks(0).topicname());
    EXPECT_EQ(3, manager._cleanAtDeleteTasks.tasks(0).dfspath_size());
    EXPECT_EQ("mock://ea117/path1/cad_topic2", manager._cleanAtDeleteTasks.tasks(0).dfspath(0));
    EXPECT_EQ("mock://ea118/path0/cad_topic2", manager._cleanAtDeleteTasks.tasks(0).dfspath(1));
    EXPECT_EQ("mock://ea119/path2/cad_topic2", manager._cleanAtDeleteTasks.tasks(0).dfspath(2));
    const string &path =
        StringUtil::formatString("%s/clean_at_delete_tasks", PathDefine::getPathFromZkPath(_zkRoot).c_str());
    CleanAtDeleteTopicTasks remoteTasks;
    EXPECT_TRUE(manager._dataAccessor->readZk(path, remoteTasks, true, false));
    EXPECT_EQ(1, remoteTasks.tasks_size());
    EXPECT_EQ("cad_topic2", remoteTasks.tasks(0).topicname());
    EXPECT_EQ(3, remoteTasks.tasks(0).dfspath_size());
    EXPECT_EQ("mock://ea117/path1/cad_topic2", remoteTasks.tasks(0).dfspath(0));
    EXPECT_EQ("mock://ea118/path0/cad_topic2", remoteTasks.tasks(0).dfspath(1));
    EXPECT_EQ("mock://ea119/path2/cad_topic2", remoteTasks.tasks(0).dfspath(2));
    manager._cleanAtDeleteTasks = emptyTasks;
    manager._dataAccessor->serializeCleanAtDeleteTasks(emptyTasks);

    manager.doPushCleanAtDeleteTopic("cad_topic2", cadTopic2Path);
    manager.doPushCleanAtDeleteTopic("cad_topic1", cadTopic1Path);
    EXPECT_EQ(2, manager._cleaningTopics.size());
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic1"));
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic2"));
    EXPECT_EQ(0, manager._cleaningTopics.count("cad_topic3"));
    ExceptionTrigger::SetExceptionCondition("remove", "mock://ea117/path1/cad_topic2", fslib::EC_OK);
    ExceptionTrigger::SetExceptionCondition("remove", "mock://ea118/path0/cad_topic2", fslib::EC_NOENT);
    ExceptionTrigger::SetExceptionCondition("remove", "mock://ea119/path2/cad_topic2", fslib::EC_NOTSUP);
    topicSet.erase("cad_topic2");
    manager.removeCleanAtDeleteTopicData(topicSet);
    EXPECT_EQ(1, manager._cleaningTopics.size());
    EXPECT_EQ(0, manager._cleaningTopics.count("cad_topic1"));
    EXPECT_EQ(1, manager._cleaningTopics.count("cad_topic2"));
    EXPECT_EQ(0, manager._cleaningTopics.count("cad_topic3"));
    EXPECT_EQ(1, manager._cleanAtDeleteTasks.tasks_size());
    EXPECT_EQ("cad_topic2", manager._cleanAtDeleteTasks.tasks(0).topicname());
    EXPECT_EQ(1, manager._cleanAtDeleteTasks.tasks(0).dfspath_size());
    EXPECT_EQ("mock://ea119/path2/cad_topic2", manager._cleanAtDeleteTasks.tasks(0).dfspath(0));
    EXPECT_TRUE(manager._dataAccessor->readZk(path, remoteTasks, true, false));
    EXPECT_EQ(1, remoteTasks.tasks_size());
    EXPECT_EQ("cad_topic2", remoteTasks.tasks(0).topicname());
    EXPECT_EQ(1, remoteTasks.tasks(0).dfspath_size());
    EXPECT_EQ("mock://ea119/path2/cad_topic2", remoteTasks.tasks(0).dfspath(0));

    ExceptionTrigger::SetExceptionCondition("remove", "mock://ea119/path2/cad_topic2", fslib::EC_OK);
    manager.removeCleanAtDeleteTopicData(topicSet);
    EXPECT_EQ(0, manager._cleaningTopics.size());
    EXPECT_EQ(0, manager._cleanAtDeleteTasks.tasks_size());
    FileSystem::_useMock = false;
    EXPECT_TRUE(manager._dataAccessor->readZk(path, remoteTasks, true, false));
    EXPECT_EQ(0, remoteTasks.tasks_size());
}

}; // namespace admin
}; // namespace swift
