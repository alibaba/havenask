#include "swift/broker/service/TransporterImpl.h"

#include <cstddef>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/broker/service/LongPollingRequestHandler.h"
#include "swift/broker/service/TopicPartitionSupervisor.h"
#include "swift/broker/service/test/FakeClosure.h"
#include "swift/config/BrokerConfig.h"
#include "swift/heartbeat/ThreadSafeTaskStatus.h"
#include "swift/heartbeat/ZkConnectionStatus.h"
#include "swift/log/BrokerLogClosure.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h" // IWYU pragma: keep
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/PermissionCenter.h"
#include "unittest/unittest.h"

using namespace std;

using namespace swift::util;
using namespace swift::protocol;
using namespace swift::heartbeat;

namespace swift {
namespace service {

class FakeLongPollingRequestHandler : public LongPollingRequestHandler {
public:
    FakeLongPollingRequestHandler(TransporterImpl *, TopicPartitionSupervisor *)
        : LongPollingRequestHandler(nullptr, nullptr) {}
    virtual ~FakeLongPollingRequestHandler() {}

public:
    void registeCallBackBeforeSend(const BrokerPartitionPtr &brokerPartition,
                                   util::ProductionLogClosure *closure) override {
        sendMessageCount++;
    }

public:
    int32_t sendMessageCount = 0;
};

class TransporterImplTest : public TESTBASE {
public:
    TransporterImplTest();
    ~TransporterImplTest();

public:
    void setUp();
    void tearDown();

protected:
    ZkConnectionStatus *_status;
    BlockPool *_centerPool;
    PermissionCenter *_permissionCenter;
    BlockPool *_fileCachePool;
    shared_ptr<FakeLongPollingRequestHandler> _fakeLongPollingRequestHandlerPtr;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, TransporterImplTest);

TransporterImplTest::TransporterImplTest()
    : _status(nullptr), _centerPool(nullptr), _permissionCenter(nullptr), _fileCachePool(nullptr) {}

TransporterImplTest::~TransporterImplTest() {}

void TransporterImplTest::setUp() {
    AUTIL_LOG(DEBUG, "setUp!");
    fslib::ErrorCode ec;
    ec = fslib::fs::FileSystem::remove(GET_TEMPLATE_DATA_PATH());
    ASSERT_TRUE(ec == fslib::EC_OK || ec == fslib::EC_NOENT);
    ec = fslib::fs::FileSystem::mkDir(GET_TEMPLATE_DATA_PATH(), true);
    ASSERT_TRUE(ec == fslib::EC_OK);
    _status = new ZkConnectionStatus;
    _fakeLongPollingRequestHandlerPtr = make_shared<FakeLongPollingRequestHandler>(nullptr, nullptr);
    _centerPool = new BlockPool(128 * 1024 * 1024, 1024 * 1024);
    _permissionCenter = new PermissionCenter();
    _fileCachePool = new BlockPool(_centerPool, 64 * 1024 * 1024, 1024 * 1024);
}

void TransporterImplTest::tearDown() {
    AUTIL_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_status);
    DELETE_AND_SET_NULL(_fileCachePool);
    DELETE_AND_SET_NULL(_permissionCenter);
    DELETE_AND_SET_NULL(_centerPool);
    _fakeLongPollingRequestHandlerPtr.reset();
}

TEST_F(TransporterImplTest, testPartitionNotRunning) {
    config::BrokerConfig brokerConfig;
    TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
    TransporterImpl transporter(&tpSupervisor);
    TaskInfo taskInfo;
    taskInfo.mutable_partitionid()->set_topicname("topic_name");
    taskInfo.mutable_partitionid()->set_id(0);
    taskInfo.set_minbuffersize(1024);
    taskInfo.set_maxbuffersize(1024);
    taskInfo.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    tpSupervisor.loadBrokerPartition(taskInfo);

    BrokerPartitionPtr partition = tpSupervisor.getBrokerPartition(taskInfo.partitionid());

    ASSERT_TRUE(partition);
    partition->_taskStatus->setPartitionStatus(PARTITION_STATUS_STARTING);

    MessageIdRequest request;
    request.set_topicname("topic_name");
    request.set_partitionid(0);
    MessageIdResponse response;
    FakeClosure closure;

    transporter.getMaxMessageId(NULL, &request, &response, &closure);
    ASSERT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_RUNNING, response.errorinfo().errcode());
    ASSERT_TRUE(closure.getDoneStatus());
}

TEST_F(TransporterImplTest, testPartitionNotExist) {
    config::BrokerConfig brokerConfig;
    TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
    TransporterImpl transporter(&tpSupervisor);
    MessageIdRequest request;
    request.set_topicname("topic_name");
    request.set_partitionid(0);
    MessageIdResponse response;
    FakeClosure closure;

    transporter.getMaxMessageId(NULL, &request, &response, &closure);
    ASSERT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, response.errorinfo().errcode());
    ASSERT_TRUE(closure.getDoneStatus());
}

TEST_F(TransporterImplTest, testInvalidRequest) {
    config::BrokerConfig brokerConfig;
    TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
    TransporterImpl transporter(&tpSupervisor);
    MessageIdRequest request;
    request.set_partitionid(0);
    MessageIdResponse response;
    FakeClosure closure;

    transporter.getMaxMessageId(NULL, &request, &response, &closure);
    ASSERT_EQ(ERROR_BROKER_REQUEST_INVALID, response.errorinfo().errcode());
    ASSERT_TRUE(closure.getDoneStatus());
}

TEST_F(TransporterImplTest, testSendMessage) {
    { // zk reject service
        config::BrokerConfig brokerConfig;
        TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
        // needRejectServing return true
        tpSupervisor._status->_heartbeatAlive = false;
        tpSupervisor._status->_zkMonitorAlive = false;

        TransporterImpl transporter(&tpSupervisor);
        ProductionRequest request;
        request.set_partitionid(0);
        MessageResponse response;
        FakeClosure closure;
        transporter._longPollingRequestHandler =
            dynamic_pointer_cast<FakeLongPollingRequestHandler>(_fakeLongPollingRequestHandlerPtr);
        ASSERT_TRUE(nullptr != transporter._longPollingRequestHandler);
        transporter.sendMessage(NULL, &request, &response, &closure);
        ASSERT_EQ(ERROR_BROKER_STOPPED, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
        ASSERT_EQ(0, _fakeLongPollingRequestHandlerPtr->sendMessageCount);
    }
    { // request invalid
        config::BrokerConfig brokerConfig;
        TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
        // needRejectServing return true
        tpSupervisor._status->_heartbeatAlive = true;
        tpSupervisor._status->_zkMonitorAlive = true;

        TransporterImpl transporter(&tpSupervisor);
        ProductionRequest request;
        request.set_partitionid(0);
        MessageResponse response;
        FakeClosure closure;
        transporter._longPollingRequestHandler =
            dynamic_pointer_cast<FakeLongPollingRequestHandler>(_fakeLongPollingRequestHandlerPtr);
        ASSERT_TRUE(nullptr != transporter._longPollingRequestHandler);
        transporter.sendMessage(NULL, &request, &response, &closure);
        ASSERT_EQ(ERROR_BROKER_REQUEST_INVALID, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
        ASSERT_EQ(0, _fakeLongPollingRequestHandlerPtr->sendMessageCount);
    }
    { // partition not found
        config::BrokerConfig brokerConfig;
        TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
        // needRejectServing return true
        tpSupervisor._status->_heartbeatAlive = true;
        tpSupervisor._status->_zkMonitorAlive = true;

        TransporterImpl transporter(&tpSupervisor);
        ProductionRequest request;
        request.set_partitionid(0);
        request.set_topicname("test");
        MessageResponse response;
        FakeClosure closure;
        transporter._longPollingRequestHandler =
            dynamic_pointer_cast<FakeLongPollingRequestHandler>(_fakeLongPollingRequestHandlerPtr);
        ASSERT_TRUE(nullptr != transporter._longPollingRequestHandler);
        transporter.sendMessage(NULL, &request, &response, &closure);
        ASSERT_EQ(ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
        ASSERT_EQ(0, _fakeLongPollingRequestHandlerPtr->sendMessageCount);
    }
    { // sealed
        config::BrokerConfig brokerConfig;
        TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
        // needRejectServing return true
        tpSupervisor._status->_heartbeatAlive = true;
        tpSupervisor._status->_zkMonitorAlive = true;
        PartitionId pid;
        pid.set_topicname("test");
        pid.set_id(0);

        TaskInfo taskInfo;
        taskInfo.mutable_partitionid()->set_topicname("test");
        taskInfo.mutable_partitionid()->set_id(0);
        ThreadSafeTaskStatusPtr tsTaskStatus(new ThreadSafeTaskStatus(taskInfo));
        tsTaskStatus->_taskStatus.set_status(PARTITION_STATUS_RUNNING);
        tsTaskStatus->_taskStatus.mutable_taskinfo()->set_sealed(true);
        BrokerPartitionPtr brokerPartition(new BrokerPartition(tsTaskStatus, NULL));
        tpSupervisor._brokerPartitionMap[pid] = make_pair(tsTaskStatus, brokerPartition);
        TransporterImpl transporter(&tpSupervisor);
        ProductionRequest request;
        request.set_partitionid(0);
        request.set_topicname("test");
        MessageResponse response;
        FakeClosure closure;
        transporter._longPollingRequestHandler =
            dynamic_pointer_cast<FakeLongPollingRequestHandler>(_fakeLongPollingRequestHandlerPtr);
        ASSERT_TRUE(nullptr != transporter._longPollingRequestHandler);

        transporter.sendMessage(NULL, &request, &response, &closure);
        ASSERT_EQ(ERROR_TOPIC_SEALED, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
        ASSERT_EQ(0, _fakeLongPollingRequestHandlerPtr->sendMessageCount);
    }
    { // decompress fail
        config::BrokerConfig brokerConfig;
        TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
        // needRejectServing return true
        tpSupervisor._status->_heartbeatAlive = true;
        tpSupervisor._status->_zkMonitorAlive = true;
        PartitionId pid;
        pid.set_topicname("test");
        pid.set_id(0);

        TaskInfo taskInfo;
        taskInfo.mutable_partitionid()->set_topicname("test");
        taskInfo.mutable_partitionid()->set_id(0);
        ThreadSafeTaskStatusPtr tsTaskStatus(new ThreadSafeTaskStatus(taskInfo));
        tsTaskStatus->_taskStatus.set_status(PARTITION_STATUS_RUNNING);
        BrokerPartitionPtr brokerPartition(new BrokerPartition(tsTaskStatus, NULL));
        tpSupervisor._brokerPartitionMap[pid] = make_pair(tsTaskStatus, brokerPartition);
        TransporterImpl transporter(&tpSupervisor);
        ProductionRequest request;
        request.set_partitionid(0);
        request.set_topicname("test");
        request.set_compressedmsgs("111"); // will decompress fail
        MessageResponse response;
        FakeClosure closure;
        transporter._longPollingRequestHandler =
            dynamic_pointer_cast<FakeLongPollingRequestHandler>(_fakeLongPollingRequestHandlerPtr);
        ASSERT_TRUE(nullptr != transporter._longPollingRequestHandler);

        transporter.sendMessage(NULL, &request, &response, &closure);
        ASSERT_EQ(ERROR_DECOMPRESS_MESSAGE, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
        ASSERT_EQ(1, _fakeLongPollingRequestHandlerPtr->sendMessageCount);
    }

    { // success
        config::BrokerConfig brokerConfig;
        TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
        // needRejectServing return true
        tpSupervisor._status->_heartbeatAlive = true;
        tpSupervisor._status->_zkMonitorAlive = true;
        PartitionId pid;
        pid.set_topicname("test");
        pid.set_id(0);

        TaskInfo taskInfo;
        taskInfo.mutable_partitionid()->set_topicname("test");
        taskInfo.mutable_partitionid()->set_id(0);
        taskInfo.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
        ThreadSafeTaskStatusPtr tsTaskStatus(new ThreadSafeTaskStatus(taskInfo));
        tsTaskStatus->_taskStatus.set_status(PARTITION_STATUS_RUNNING);
        BrokerPartitionPtr brokerPartition(new BrokerPartition(tsTaskStatus, NULL));

        ASSERT_TRUE(brokerPartition->init(brokerConfig, _centerPool, _fileCachePool, _permissionCenter));

        tpSupervisor._brokerPartitionMap[pid] = make_pair(tsTaskStatus, brokerPartition);
        TransporterImpl transporter(&tpSupervisor);
        ProductionRequest request;
        request.set_partitionid(0);
        request.set_topicname("test");
        Message *message = request.add_msgs();
        message->set_data("111");

        MessageResponse response;
        FakeClosure closure;
        transporter._longPollingRequestHandler =
            dynamic_pointer_cast<FakeLongPollingRequestHandler>(_fakeLongPollingRequestHandlerPtr);
        ASSERT_TRUE(nullptr != transporter._longPollingRequestHandler);

        transporter.sendMessage(NULL, &request, &response, &closure);
        ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
        ASSERT_EQ(2, _fakeLongPollingRequestHandlerPtr->sendMessageCount);
    }

    { // success in security mode
        config::BrokerConfig brokerConfig;
        brokerConfig.dfsRoot = GET_TEMPLATE_DATA_PATH();
        TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
        // needRejectServing return true
        tpSupervisor._status->_heartbeatAlive = true;
        tpSupervisor._status->_zkMonitorAlive = true;
        PartitionId pid;
        pid.set_topicname("test");
        pid.set_id(0);

        TaskInfo taskInfo;
        taskInfo.mutable_partitionid()->set_topicname("test");
        taskInfo.mutable_partitionid()->set_id(0);
        taskInfo.set_topicmode(TOPIC_MODE_SECURITY);
        ThreadSafeTaskStatusPtr tsTaskStatus(new ThreadSafeTaskStatus(taskInfo));
        tsTaskStatus->_taskStatus.set_status(PARTITION_STATUS_RUNNING);
        BrokerPartitionPtr brokerPartition(new BrokerPartition(tsTaskStatus, NULL));

        ASSERT_TRUE(brokerPartition->init(brokerConfig, _centerPool, _fileCachePool, _permissionCenter));

        tpSupervisor._brokerPartitionMap[pid] = make_pair(tsTaskStatus, brokerPartition);
        TransporterImpl transporter(&tpSupervisor);
        ProductionRequest request;
        request.set_partitionid(0);
        request.set_topicname("test");
        Message *message = request.add_msgs();
        message->set_data("111");

        MessageResponse response;
        FakeClosure closure;
        transporter._longPollingRequestHandler =
            dynamic_pointer_cast<FakeLongPollingRequestHandler>(_fakeLongPollingRequestHandlerPtr);
        ASSERT_TRUE(nullptr != transporter._longPollingRequestHandler);

        transporter.sendMessage(NULL, &request, &response, &closure);
        usleep(1000 * 1000); // wait util message committed in security mode.
        ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
        ASSERT_EQ(3, _fakeLongPollingRequestHandlerPtr->sendMessageCount);
    }
}

TEST_F(TransporterImplTest, testSessionChanged) {
    config::BrokerConfig brokerConfig;
    TopicPartitionSupervisor tpSupervisor(&brokerConfig, _status, NULL);
    // needRejectServing return true
    tpSupervisor._status->_heartbeatAlive = true;
    tpSupervisor._status->_zkMonitorAlive = true;
    PartitionId pid;
    pid.set_topicname("test");
    pid.set_id(0);
    TaskInfo taskInfo;
    taskInfo.mutable_partitionid()->set_topicname("test");
    taskInfo.mutable_partitionid()->set_id(0);
    ThreadSafeTaskStatusPtr tsTaskStatus(new ThreadSafeTaskStatus(taskInfo));
    tsTaskStatus->_taskStatus.set_status(PARTITION_STATUS_RUNNING);
    BrokerPartitionPtr brokerPartition(new BrokerPartition(tsTaskStatus, NULL));
    tpSupervisor._brokerPartitionMap[pid] = make_pair(tsTaskStatus, brokerPartition);
    TransporterImpl transporter(&tpSupervisor);
    { // 1. getMessage
        ConsumptionRequest request;
        request.set_partitionid(0);
        request.set_startid(0);
        request.set_topicname("test");
        request.set_sessionid(123);
        MessageResponse response;
        FakeClosure closure;
        transporter.getMessage(NULL, &request, &response, &closure);
        ASSERT_EQ(ERROR_BROKER_SESSION_CHANGED, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
    }
    { // 2. sendMessage
        ProductionRequest request;
        request.set_partitionid(0);
        request.set_topicname("test");
        request.set_compressedmsgs("111"); // will decompress fail
        request.set_sessionid(123);
        MessageResponse response;
        FakeClosure closure;
        transporter.sendMessage(NULL, &request, &response, &closure);
        ASSERT_EQ(ERROR_BROKER_SESSION_CHANGED, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
    }
    { // 3. getMaxMessageId
        MessageIdRequest request;
        request.set_partitionid(0);
        request.set_topicname("test");
        request.set_timestamp(163444000);
        request.set_sessionid(123);
        MessageIdResponse response;
        FakeClosure closure;
        transporter.getMaxMessageId(NULL, &request, &response, &closure);
        ASSERT_EQ(ERROR_BROKER_SESSION_CHANGED, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
    }
    { // 4. getMinMessageIdByTime
        MessageIdRequest request;
        request.set_partitionid(0);
        request.set_topicname("test");
        request.set_timestamp(163444000);
        request.set_sessionid(123);
        MessageIdResponse response;
        FakeClosure closure;
        transporter.getMinMessageIdByTime(NULL, &request, &response, &closure);
        ASSERT_EQ(ERROR_BROKER_SESSION_CHANGED, response.errorinfo().errcode());
        ASSERT_TRUE(closure.getDoneStatus());
    }
}

TEST_F(TransporterImplTest, testUnauthorizedRequest) {}

} // namespace service
} // namespace swift
