#include "swift/broker/service/BrokerPartition.h"

#include <algorithm>
#include <assert.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-nice-strict.h>
#include <gtest/gtest-matchers.h>
#include <limits>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include <zlib.h>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "flatbuffers/flatbuffers.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/broker/replication/MessageProducer.h"
#include "swift/broker/service/MirrorPartition.h"
#include "swift/broker/service/test/FakeClosure.h"
#include "swift/broker/storage/MessageBrain.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupWriter.h"
#include "swift/common/RangeUtil.h"
#include "swift/config/BrokerConfig.h"
#include "swift/config/ConfigDefine.h"
#include "swift/config/PartitionConfig.h"
#include "swift/heartbeat/ThreadSafeTaskStatus.h"
#include "swift/log/BrokerLogClosure.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageCompressor.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/protocol/test/MessageCreator.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/MessageUtil.h"
#include "swift/util/PermissionCenter.h"
#include "swift/util/ZkDataAccessor.h"
#include "unittest/unittest.h"

namespace swift {
namespace monitor {
class BrokerMetricsReporter;
struct ReadMetricsCollector;
} // namespace monitor
namespace network {
class SwiftRpcChannelManager;
} // namespace network
namespace util {
class TimeoutChecker;
} // namespace util
} // namespace swift

using namespace std;
using namespace autil;
using namespace swift::config;
using namespace swift::util;
using namespace swift::protocol;
using namespace swift::storage;
using namespace swift::common;
using namespace swift::heartbeat;

namespace swift {
namespace service {

class MockMessageBrain : public swift::storage::MessageBrain {
public:
    MOCK_METHOD5(getMessage,
                 protocol::ErrorCode(const protocol::ConsumptionRequest *request,
                                     protocol::MessageResponse *response,
                                     util::TimeoutChecker *timeoutChecker,
                                     const std::string *srcIpPort,
                                     monitor::ReadMetricsCollector &collector));
};

namespace __detail {
class MockBrokerPartition : public BrokerPartition {
public:
    MockBrokerPartition(const protocol::TaskInfo &taskInfo,
                        monitor::BrokerMetricsReporter *metricsReporter = nullptr,
                        util::ZkDataAccessorPtr zkDataAccessor = nullptr)
        : BrokerPartition(taskInfo, metricsReporter, zkDataAccessor) {}
    MockBrokerPartition(const ThreadSafeTaskStatusPtr &taskInfo,
                        monitor::BrokerMetricsReporter *metricsReporter = nullptr,
                        util::ZkDataAccessorPtr zkDataAccessor = nullptr)
        : BrokerPartition(taskInfo, metricsReporter, zkDataAccessor) {}

public:
    MOCK_CONST_METHOD1(createMirrorPartition, std::unique_ptr<MirrorPartition>(const PartitionId &));
};
using NiceMockBrokerPartition = ::testing::NiceMock<MockBrokerPartition>;
} // namespace __detail

class MockMirrorPartition : public MirrorPartition {
public:
    MockMirrorPartition(const protocol::PartitionId &pid) : MirrorPartition(pid) {}

public:
    MOCK_CONST_METHOD2(createProducer,
                       std::unique_ptr<replication::MessageProducer>(
                           const std::string &, const std::shared_ptr<network::SwiftRpcChannelManager> &));
};
using NiceMockMirrorPartition = ::testing::NiceMock<MockMirrorPartition>;

class BrokerPartitionTest : public TESTBASE {
public:
    void setUp() override {
        _centerPool = std::make_unique<BlockPool>(128 * 1024 * 1024, 1024 * 1024);
        _permissionCenter = std::make_unique<PermissionCenter>();
        _fileCachePool = std::make_unique<BlockPool>(_centerPool.get(), 64 * 1024 * 1024, 1024 * 1024);
    }

    void tearDown() override {
        _fileCachePool.reset();
        _centerPool.reset();
    }

protected:
    TaskInfo makeTaskInfo(const std::string &pidStr,
                          TopicMode topicMode,
                          int64_t minBufferSize,
                          int64_t maxBufferSize,
                          const std::string &dfsRoot = "") const {
        TaskInfo taskInfo;
        *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId(pidStr);
        taskInfo.set_topicmode(topicMode);
        taskInfo.set_minbuffersize(minBufferSize);
        taskInfo.set_maxbuffersize(maxBufferSize);
        if (!dfsRoot.empty()) {
            taskInfo.set_dfsroot(dfsRoot);
        }
        return taskInfo;
    }
    protocol::ErrorCode getMessage(BrokerPartition &partition, uint64_t startId, uint32_t count);
    protocol::ErrorCode sendMessage(BrokerPartition &partition, uint32_t count);
    protocol::ErrorCode sendMessage(BrokerPartition &partition,
                                    uint32_t count,
                                    const std::string &data,
                                    bool compressMsg = false,
                                    bool compressRequest = false,
                                    bool errorMsg = false,
                                    bool mergeMsg = false);
    protocol::ErrorCode getMaxMessageId(BrokerPartition &partition, int64_t &msgId);
    protocol::ErrorCode getMinMessageIdByTime(BrokerPartition &partition, int64_t timestamp, int64_t &msgId);

protected:
    config::BrokerConfig _config;
    std::unique_ptr<BlockPool> _centerPool;
    std::unique_ptr<PermissionCenter> _permissionCenter;
    std::unique_ptr<BlockPool> _fileCachePool;
};

TEST_F(BrokerPartitionTest, testStartMirrorPartition) {
    // topic mode incorrect
    auto taskInfo = makeTaskInfo("topic1_1_100_200", TOPIC_MODE_MEMORY_ONLY, 128, 128);
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo);
    ASSERT_FALSE(brokerPart->startMirrorPartition("mirrorZkRoot", nullptr));
}

// TEST_F(BrokerPartitionTest, testInitMirror) {
//     auto dataDir = GET_TEMPLATE_DATA_PATH();
//     auto taskInfo = makeTaskInfo("topic1_1_100", TOPIC_MODE_SECURITY, 128, 128, dataDir);
//     auto brokerPart = std::make_unique<__detail::NiceMockBrokerPartition>(taskInfo);
//     auto mirror = std::make_unique<NiceMockMirrorPartition>(taskInfo.partitionid());
//     auto *mockMirror = mirror.get();
//     EXPECT_CALL(*brokerPart, createMirrorPartition(_)).WillOnce(Return(ByMove(std::move(mirror))));
//     auto producer = std::make_unique<replication::NiceMockMessageProducer>(100, true);
//     auto *mockProducer = producer.get();
//     EXPECT_CALL(*mockMirror, createProducer(_, _)).WillOnce(Return(ByMove(std::move(producer))));

//     std::atomic<int> flag{0};
//     auto produceFn = [&flag](protocol::ConsumptionRequest *request, protocol::MessageResponse *response) {
//         response->mutable_errorinfo()->set_errcode(ERROR_BROKER_NO_DATA);
//         int v = flag.load();
//         if (v == 0) { // unknown
//             return;
//         } else if (v == 1) {
//             // leader
//             response->set_selfversion(99);
//             response->set_maxmsgid(100);
//         } else {
//             // follower
//             response->set_selfversion(101);
//             response->set_maxmsgid(100);
//         }
//     };
//     EXPECT_CALL(*mockProducer, produce(_, _)).WillRepeatedly(Invoke(std::move(produceFn)));
//     _config.mirrorZkRoot = "zfs://xxx";
//     ASSERT_TRUE(brokerPart->init(_config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
//     ASSERT_EQ(PARTITION_STATUS_RECOVERING, brokerPart->getPartitionStatus());
//     ASSERT_TRUE(brokerPart->_mirror);
//     ASSERT_FALSE(brokerPart->_mirror->canServe());
//     ASSERT_FALSE(brokerPart->_mirror->canWrite());
//     flag.store(2);
//     sleep(2); // wait call back
//     EXPECT_TRUE(brokerPart->_mirror);
//     EXPECT_TRUE(brokerPart->_mirror->canServe());
//     EXPECT_FALSE(brokerPart->_mirror->canWrite());
//     EXPECT_EQ(PARTITION_STATUS_RUNNING, brokerPart->getPartitionStatus());
// }

TEST_F(BrokerPartitionTest, testSendAndGetMessage) {
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_MEMORY_ONLY, 128, 128);
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo);
    ASSERT_TRUE(brokerPart->init(_config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
    ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, brokerPart->_messageBrain->getTopicMode());
    ASSERT_TRUE(brokerPart->_writerVersionController == nullptr);

    int64_t msgId;
    ASSERT_EQ(ERROR_BROKER_NO_DATA, getMessage(*brokerPart, 0, 100));
    ASSERT_EQ(ERROR_BROKER_NO_DATA, getMaxMessageId(*brokerPart, msgId));
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 100));
    ASSERT_EQ(ERROR_NONE, getMaxMessageId(*brokerPart, msgId));
    ASSERT_EQ(int64_t(99), msgId);
    ASSERT_EQ(ERROR_NONE, getMessage(*brokerPart, 0, 100));
    ASSERT_EQ(ERROR_NONE, getMessage(*brokerPart, 0, 100));
}

TEST_F(BrokerPartitionTest, testPreparePartitionConfig) {
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_NORMAL, 1024, 1024);
    BrokerPartition brokerPart(taskInfo);
    {
        BrokerConfig brokerConfig;
        string path = "/NNNN";
        brokerConfig.dfsRoot = path;
        PartitionConfig partConfig;
        brokerPart.preparePartitionConfig(brokerConfig, partConfig);
        ASSERT_EQ(string("/NNNN/topic1/1/"), partConfig.getDataRoot());
        ASSERT_EQ((int64_t)1024 * 1024 * 1024, partConfig.getPartitionMinBufferSize());
        ASSERT_EQ((int64_t)1024 * 1024 * 1024, partConfig.getPartitionMaxBufferSize());
        ASSERT_EQ(DEFAULT_BUFFER_BLOCK_SIZE - 0, partConfig.getBlockSize());
    }
    {
        BrokerConfig brokerConfig;
        brokerConfig.setBufferBlockSize(0.00001);
        PartitionConfig partConfig;
        brokerPart.preparePartitionConfig(brokerConfig, partConfig);
        ASSERT_EQ((int64_t)1024 * 1024 * 1024, partConfig.getPartitionMinBufferSize());
        ASSERT_EQ((int64_t)1024 * 1024 * 1024, partConfig.getPartitionMaxBufferSize());
        ASSERT_EQ(MIN_BLOCK_SIZE, partConfig.getBlockSize());
    }
    {
        BrokerConfig brokerConfig;
        brokerConfig.dfsRoot = "dataRoot";
        PartitionConfig partConfig;
        brokerPart.preparePartitionConfig(brokerConfig, partConfig);
        ASSERT_EQ(string("dataRoot/topic1/1/"), partConfig.getDataRoot());
    }
    { // set dfs root
        auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_NORMAL, 1024, 1024, "newDataRoot");
        BrokerPartition brokerPart(taskInfo);
        BrokerConfig brokerConfig;
        brokerConfig.dfsRoot = "dataRoot";
        PartitionConfig partConfig;
        brokerPart.preparePartitionConfig(brokerConfig, partConfig);
        ASSERT_EQ(string("newDataRoot/topic1/1/"), partConfig.getDataRoot());
    }
    { // set extend dfs root
        TaskInfo taskInfo;
        *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic1_1");
        string *oldroot1 = taskInfo.add_extenddfsroot();
        *oldroot1 = "oldDataRoot1";
        string *oldroot2 = taskInfo.add_extenddfsroot();
        *oldroot2 = "dataRoot";
        ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
        BrokerPartition brokerPart(threadSafeTaskStatus);
        BrokerConfig brokerConfig;
        brokerConfig.dfsRoot = "dataRoot";
        PartitionConfig partConfig;
        brokerPart.preparePartitionConfig(brokerConfig, partConfig);
        ASSERT_EQ(string("dataRoot/topic1/1/"), partConfig.getDataRoot());
        vector<string> oldDataRoots = partConfig.getExtendDataRoots();
        ASSERT_EQ(size_t(1), oldDataRoots.size());
        ASSERT_EQ(string("oldDataRoot1/topic1/1/"), oldDataRoots[0]);
    }
    { // set extend dfs root
        TaskInfo taskInfo;
        *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic1_1");
        taskInfo.set_dfsroot("dataRoot");
        string *oldroot1 = taskInfo.add_extenddfsroot();
        *oldroot1 = "oldDataRoot1";
        string *oldroot2 = taskInfo.add_extenddfsroot();
        *oldroot2 = "dataRoot";
        ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
        BrokerPartition brokerPart(threadSafeTaskStatus);
        BrokerConfig brokerConfig;
        brokerConfig.dfsRoot = "dataRoot_local";
        PartitionConfig partConfig;
        brokerPart.preparePartitionConfig(brokerConfig, partConfig);
        ASSERT_EQ(string("dataRoot/topic1/1/"), partConfig.getDataRoot());
        vector<string> oldDataRoots = partConfig.getExtendDataRoots();
        ASSERT_EQ(size_t(1), oldDataRoots.size());
        ASSERT_EQ(string("oldDataRoot1/topic1/1/"), oldDataRoots[0]);
    }
}

TEST_F(BrokerPartitionTest, testInitWithSecurityMode) {
    string dataDir = GET_TEMPLATE_DATA_PATH() + string("/testInitWithSecurityMode/");
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_SECURITY, 1024, 1024, dataDir);
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo);
    ASSERT_TRUE(brokerPart->init(_config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
    ASSERT_EQ(TOPIC_MODE_SECURITY, brokerPart->_messageBrain->getTopicMode());

    fslib::ErrorCode ec = fslib::fs::FileSystem::remove(dataDir);
    ASSERT_TRUE(ec == fslib::EC_OK || ec == fslib::EC_NOENT);
}

TEST_F(BrokerPartitionTest, testGetMessageWithCompress) {
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_MEMORY_ONLY, 1024, 1024);
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo);
    ASSERT_TRUE(brokerPart->init(_config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
    ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, brokerPart->_messageBrain->getTopicMode());

    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 100));

    int64_t msgId;
    ASSERT_EQ(ERROR_NONE, getMaxMessageId(*brokerPart, msgId));
    ASSERT_EQ(int64_t(99), msgId);

    ConsumptionRequest request;
    request.set_startid(0);
    request.set_count(100);
    request.set_needcompress(true);
    MessageResponse response;
    ErrorCode ec = brokerPart->getMessage(&request, &response);
    ASSERT_EQ(ERROR_NONE, ec);

    ASSERT_EQ(0, response.msgs_size());
    string compressedMsgs = response.compressedmsgs();
    autil::ZlibCompressor compressor(Z_BEST_SPEED);
    compressor.addDataToBufferIn(compressedMsgs);
    ASSERT_TRUE(compressor.decompress());
    string decompressedMsgs;
    decompressedMsgs.assign(compressor.getBufferOut(), compressor.getBufferOutLen());
    Messages messages;
    ASSERT_TRUE(messages.ParseFromString(decompressedMsgs));
    ASSERT_EQ(100, messages.msgs_size());
    for (int i = 0; i < messages.msgs_size(); ++i) {
        ASSERT_EQ(string("aaa"), messages.msgs(i).data());
    }
}

TEST_F(BrokerPartitionTest, testCompressRequestLevel) {
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_MEMORY_ONLY, 1024, 1024);
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo);
    ASSERT_TRUE(brokerPart->init(_config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
    ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, brokerPart->_messageBrain->getTopicMode());
    string data = "bbbadeddfasdfewerfggg";
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 100, data, false, true));

    int64_t msgId;
    ASSERT_EQ(ERROR_NONE, getMaxMessageId(*brokerPart, msgId));
    ASSERT_EQ(int64_t(99), msgId);

    ConsumptionRequest request;
    request.set_startid(0);
    request.set_count(100);
    request.set_needcompress(true);
    MessageResponse response;
    ErrorCode ec = brokerPart->getMessage(&request, &response);
    ASSERT_EQ(ERROR_NONE, ec);

    ASSERT_EQ(0, response.msgs_size());
    float ratio;
    MessageCompressor::decompressMessageResponse(&response, ratio);
    ASSERT_EQ(100, response.msgs_size());
    for (int i = 0; i < response.msgs_size(); ++i) {
        ASSERT_EQ(data, response.msgs(i).data());
    }
}

TEST_F(BrokerPartitionTest, testCompressMessageLevel) {
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_MEMORY_ONLY, 1024, 1024);
    taskInfo.set_compressmsg(true); // compess single message
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo);
    ASSERT_TRUE(brokerPart->init(_config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
    ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, brokerPart->_messageBrain->getTopicMode());
    string data(1000, 'a');
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 50, data, false, false)); // compress message in broker
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 50, data, true, false));  // compress message in client

    int64_t msgId;
    ASSERT_EQ(ERROR_NONE, getMaxMessageId(*brokerPart, msgId));
    ASSERT_EQ(int64_t(99), msgId);
    { // decompress in broker
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);

        ASSERT_EQ(100, response.msgs_size());
        for (int i = 0; i < response.msgs_size(); ++i) {
            ASSERT_TRUE(!response.msgs(i).compress());
            ASSERT_EQ(data, response.msgs(i).data());
        }
    }
    { // decompress in client
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(100);
        request.set_candecompressmsg(true);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(100, response.msgs_size());
        for (int i = 0; i < response.msgs_size(); ++i) {
            ASSERT_TRUE(response.msgs(i).compress());
        }
        MessageCompressor::decompressResponseMessage(&response);
        for (int i = 0; i < response.msgs_size(); ++i) {
            ASSERT_TRUE(!response.msgs(i).compress());
            ASSERT_EQ(data, response.msgs(i).data());
        }
    }
}

TEST_F(BrokerPartitionTest, testCompressMessageLevelFiledFilter) {
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_MEMORY_ONLY, 1024, 1024);
    taskInfo.set_compressmsg(true);     // compess single message
    taskInfo.set_needfieldfilter(true); // field filter mode
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo);
    ASSERT_TRUE(brokerPart->init(_config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
    ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, brokerPart->_messageBrain->getTopicMode());
    FieldGroupWriter fieldGroupWriter;
    string name1 = string("CMD");
    string value1 = string("DEL");
    fieldGroupWriter.addProductionField(name1, value1, false);
    string name2 = string("field1");
    string value2 = string("abc");
    fieldGroupWriter.addProductionField(name2, value2, true);
    string name3 = string("field2");
    string value3 = string("def");
    fieldGroupWriter.addProductionField(name3, value3, false);
    string data = fieldGroupWriter.toString();
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 50, data, false, false)); // compress message in broker
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 50, data, true, false));  // compress message in client
    int64_t msgId;
    ASSERT_EQ(ERROR_NONE, getMaxMessageId(*brokerPart, msgId));
    ASSERT_EQ(int64_t(99), msgId);

    fieldGroupWriter.reset();
    StringView const_value2(value2);
    StringView const_value3(value3);
    fieldGroupWriter.addConsumptionField(&const_value2, true);
    fieldGroupWriter.addConsumptionField(&const_value3, false);
    string returnData = fieldGroupWriter.toString();
    ConsumptionRequest request;
    *request.add_requiredfieldnames() = "field1";
    *request.add_requiredfieldnames() = "field2";
    request.set_startid(0);
    request.set_count(100);
    { // decompress in broker
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);

        ASSERT_EQ(100, response.msgs_size());
        for (int i = 0; i < response.msgs_size(); ++i) {
            ASSERT_TRUE(!response.msgs(i).compress());
            ASSERT_EQ(returnData, response.msgs(i).data());
        }
    }
    { // require decompress in client, but still decompress in broker
        request.set_candecompressmsg(true);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(100, response.msgs_size());
        for (int i = 0; i < response.msgs_size(); ++i) {
            ASSERT_TRUE(!response.msgs(i).compress());
            ASSERT_EQ(returnData, response.msgs(i).data());
        }
    }
}

TEST_F(BrokerPartitionTest, testCompressMessageLevelWithErrorDecodeError) {
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_MEMORY_ONLY, 1024, 1024);
    taskInfo.set_compressmsg(true); // compess single message
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo);
    ASSERT_TRUE(brokerPart->init(_config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
    ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, brokerPart->_messageBrain->getTopicMode());
    string data(1000, 'a');
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 20, data, false, false));       // compress message in broker
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 50, data, true, false));        // compress message in client
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 30, data, false, false, true)); // error message

    int64_t msgId;
    ASSERT_EQ(ERROR_NONE, getMaxMessageId(*brokerPart, msgId));
    ASSERT_EQ(int64_t(99), msgId);
    { // decompress in broker
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);

        ASSERT_EQ(100, response.msgs_size());
        for (int i = 0; i < response.msgs_size(); ++i) {
            if (i < 70) {
                ASSERT_TRUE(!response.msgs(i).compress());
            } else {
                ASSERT_TRUE(response.msgs(i).compress()); // error msg
            }
            ASSERT_EQ(data, response.msgs(i).data());
        }
    }
    { // decompress in client
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(100);
        request.set_candecompressmsg(true);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(100, response.msgs_size());
        for (int i = 0; i < response.msgs_size(); ++i) {
            ASSERT_TRUE(response.msgs(i).compress());
        }
        MessageCompressor::decompressResponseMessage(&response);
        for (int i = 0; i < response.msgs_size(); ++i) {
            if (i < 70) {
                ASSERT_TRUE(!response.msgs(i).compress());
            } else {
                ASSERT_TRUE(response.msgs(i).compress()); // error msg
            }
            ASSERT_EQ(data, response.msgs(i).data());
        }
    }
}

TEST_F(BrokerPartitionTest, testSendAndGetMergeMessage_PB) {
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_MEMORY_ONLY, 128, 128);
    taskInfo.set_needfieldfilter(true);
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo);
    config::BrokerConfig config;
    config.checkFieldFilterMsg = false; // true will uncompress message
    ASSERT_TRUE(brokerPart->init(config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
    ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, brokerPart->_messageBrain->getTopicMode());

    int64_t msgId;
    FieldGroupWriter writer;
    string data(100000, 'a');
    writer.addProductionField("field_a", data, true);
    writer.addProductionField("field_b", "1", true);
    data = writer.toString();
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 61, data, true, true, false, true));
    ASSERT_EQ(ERROR_NONE, getMaxMessageId(*brokerPart, msgId));
    ASSERT_EQ(int64_t(15), msgId);
    { // client not support merge[false] decompress[false]
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(61, response.msgs_size());
        for (int i = 0; i < 61; i++) {
            ASSERT_TRUE(!response.msgs(i).compress());
            ASSERT_TRUE(!response.msgs(i).merged());
            ASSERT_EQ(i, response.msgs(i).uint8maskpayload());
            ASSERT_EQ(i * 1024, response.msgs(i).uint16payload());
            ASSERT_EQ(data, response.msgs(i).data());
        }
    }
    { // client not support merge[false] decompress[true]
        ConsumptionRequest request;
        request.set_candecompressmsg(true);
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(61, response.msgs_size());
        for (int i = 0; i < 61; i++) {
            if (i == 60) {
                ASSERT_TRUE(response.msgs(i).compress()); // last message not merged
            } else {
                ASSERT_TRUE(!response.msgs(i).compress());
                ASSERT_TRUE(!response.msgs(i).merged());
                ASSERT_EQ(i, response.msgs(i).uint8maskpayload());
                ASSERT_EQ(i * 1024, response.msgs(i).uint16payload());
                ASSERT_EQ(data, response.msgs(i).data());
            }
        }
    }
    { // client support merge[true] decompress[false]
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(16, response.msgs_size());
        for (int i = 0; i < 16; i++) {
            ASSERT_TRUE(!response.msgs(i).compress());
            if (i < 8) {
                ASSERT_TRUE(response.msgs(i).merged());
                ASSERT_EQ(32767, response.msgs(i).uint16payload()) << i;
                ASSERT_EQ(data.size() * 4 + 34, response.msgs(i).data().size());

            } else if (i == 15) {
                ASSERT_TRUE(!response.msgs(i).merged());
                ASSERT_EQ(61440, response.msgs(i).uint16payload()) << i;
                ASSERT_EQ(data, response.msgs(i).data());

            } else {
                ASSERT_TRUE(response.msgs(i).merged());
                ASSERT_EQ(65535, response.msgs(i).uint16payload()) << i;
                ASSERT_EQ(data.size() * 4 + 34, response.msgs(i).data().size());
            }
        }
    }
    { // client support merge [true] decompress [true]
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.set_candecompressmsg(true);
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(16, response.msgs_size());
        for (int i = 0; i < 16; i++) {
            ASSERT_TRUE(response.msgs(i).compress()) << i;
            if (i < 8) {
                ASSERT_TRUE(response.msgs(i).merged());
                ASSERT_EQ(32767, response.msgs(i).uint16payload()) << i;
                ASSERT_TRUE(data.size() * 4 + 34 != response.msgs(i).data().size());
            } else if (i == 15) {
                ASSERT_TRUE(!response.msgs(i).merged());
                ASSERT_EQ(61440, response.msgs(i).uint16payload()) << i;
                ASSERT_TRUE(data != response.msgs(i).data());
            } else {
                ASSERT_TRUE(response.msgs(i).merged());
                ASSERT_EQ(65535, response.msgs(i).uint16payload()) << i;
                ASSERT_TRUE(data.size() * 4 + 34 != response.msgs(i).data().size());
            }
        }
    }

    { // client support merge [false] with meta filter
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(100);
        Filter filter;
        filter.set_from(27648); // 27
        filter.set_to(43008);   // 42
        *request.mutable_filter() = filter;
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(16, response.msgs_size());
        for (int i = 0; i < 16; i++) {
            ASSERT_TRUE(!response.msgs(i).merged()) << i;
            ASSERT_TRUE(!response.msgs(i).compress()) << i;
            ASSERT_EQ(i + 27, response.msgs(i).uint8maskpayload());
            ASSERT_EQ((i + 27) * 1024, response.msgs(i).uint16payload());
            ASSERT_EQ(data, response.msgs(i).data());
        }
    }
    { // client support merge [false] with meta filter
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(100);
        Filter filter;
        filter.set_from(27648); // 27
        filter.set_to(65535);   //
        *request.mutable_filter() = filter;
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(34, response.msgs_size());
        for (int i = 0; i < 34; i++) {
            ASSERT_TRUE(!response.msgs(i).merged()) << i;
            ASSERT_TRUE(!response.msgs(i).compress()) << i;
            ASSERT_EQ(i + 27, response.msgs(i).uint8maskpayload());
            ASSERT_EQ((i + 27) * 1024, response.msgs(i).uint16payload());
            ASSERT_EQ(data, response.msgs(i).data());
        }
    }

    { // client support merge [false] with field filter
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(100);
        request.set_fieldfilterdesc("field_b IN 1");
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(61, response.msgs_size());
        for (int i = 0; i < 61; i++) {
            ASSERT_TRUE(!response.msgs(i).merged()) << i;
            ASSERT_TRUE(!response.msgs(i).compress()) << i;
            ASSERT_EQ(i, response.msgs(i).uint8maskpayload());
            ASSERT_EQ(i * 1024, response.msgs(i).uint16payload());
            ASSERT_EQ(data, response.msgs(i).data());
        }
    }
    { // client support merge [false] with field filter
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(100);
        request.set_fieldfilterdesc("field_b IN 0");
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(0, response.msgs_size());
    }
    { // client support merge [true] with meta filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.set_startid(0);
        request.set_count(100);
        Filter filter;
        filter.set_from(27648); // 27
        filter.set_to(43008);   // 42
        *request.mutable_filter() = filter;
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(15, response.msgs_size());
        for (int i = 0; i < 15; i++) {
            ASSERT_TRUE(response.msgs(i).merged()) << i;
            ASSERT_TRUE(!response.msgs(i).compress()) << i;
            if (i < 8) {
                ASSERT_EQ(32767, response.msgs(i).uint16payload()) << i;
            } else {
                ASSERT_EQ(65535, response.msgs(i).uint16payload()) << i;
            }
            ASSERT_EQ(data.size() * 4 + 34, response.msgs(i).data().size());
        }
    }
    { // client support merge [true] with meta filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.set_startid(0);
        request.set_count(100);
        Filter filter;
        filter.set_from(43008); // 27
        filter.set_to(43008);   // 42
        *request.mutable_filter() = filter;
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(7, response.msgs_size());
        for (int i = 0; i < 7; i++) {
            ASSERT_TRUE(response.msgs(i).merged()) << i;
            ASSERT_TRUE(!response.msgs(i).compress()) << i;
            ASSERT_EQ(65535, response.msgs(i).uint16payload()) << i;
            ASSERT_EQ(data.size() * 4 + 34, response.msgs(i).data().size());
        }
    }
    { // client support merge [true] with filed filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.set_fieldfilterdesc("field_b IN 0");
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(15, response.msgs_size());
        for (int i = 0; i < 15; i++) {
            ASSERT_TRUE(response.msgs(i).merged()) << i;
            ASSERT_TRUE(!response.msgs(i).compress()) << i;
        }
    }
    { // client support merge [true] compress [true] with meta filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.set_candecompressmsg(true);
        request.set_startid(0);
        request.set_count(100);
        Filter filter;
        filter.set_from(43008); // 27
        filter.set_to(43008);   // 42
        *request.mutable_filter() = filter;
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(7, response.msgs_size());
        for (int i = 0; i < 7; i++) {
            ASSERT_TRUE(response.msgs(i).merged()) << i;
            ASSERT_TRUE(response.msgs(i).compress()) << i;
            ASSERT_EQ(65535, response.msgs(i).uint16payload()) << i;
            ASSERT_TRUE(data.size() * 4 + 34 > response.msgs(i).data().size());
        }
    }
    { // client support merge [true] compress [true] with filed filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.set_candecompressmsg(true);
        request.set_fieldfilterdesc("field_b IN 0");
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(15, response.msgs_size());
        for (int i = 0; i < 15; i++) {
            ASSERT_TRUE(response.msgs(i).merged()) << i;
            ASSERT_TRUE(response.msgs(i).compress()) << i;
        }
    }
}

TEST_F(BrokerPartitionTest, testSendAndGetMergeMessage_FB) {
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_MEMORY_ONLY, 128, 128);
    taskInfo.set_needfieldfilter(true);
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo);
    config::BrokerConfig config;
    config.checkFieldFilterMsg = false; // true will uncompress message
    ASSERT_TRUE(brokerPart->init(config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
    ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, brokerPart->_messageBrain->getTopicMode());

    int64_t msgId;
    FieldGroupWriter writer;
    string data(100000, 'a');
    writer.addProductionField("field_a", data, true);
    writer.addProductionField("field_b", "1", true);
    data = writer.toString();
    ASSERT_EQ(ERROR_NONE, sendMessage(*brokerPart, 61, data, true, true, false, true));
    ASSERT_EQ(ERROR_NONE, getMaxMessageId(*brokerPart, msgId));
    ASSERT_EQ(int64_t(15), msgId);
    { // client not support merge[false] decompress[false]
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(61, reader.size());
        for (int i = 0; i < 61; i++) {
            ASSERT_TRUE(!reader.read(i)->compress());
            ASSERT_TRUE(!reader.read(i)->merged());
            ASSERT_EQ(i, reader.read(i)->uint8MaskPayload());
            ASSERT_EQ(i * 1024, reader.read(i)->uint16Payload());
            ASSERT_EQ(data, reader.read(i)->data()->str());
        }
    }
    { // client not support merge[false] decompress[true]
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_candecompressmsg(true);
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(61, reader.size());
        for (int i = 0; i < 61; i++) {
            if (i == 60) {
                ASSERT_TRUE(reader.read(i)->compress()); // last message not merged
            } else {
                ASSERT_TRUE(!reader.read(i)->compress());
                ASSERT_TRUE(!reader.read(i)->merged());
                ASSERT_EQ(i, reader.read(i)->uint8MaskPayload());
                ASSERT_EQ(i * 1024, reader.read(i)->uint16Payload());
                ASSERT_EQ(data, reader.read(i)->data()->str());
            }
        }
    }
    { // client support merge[true] decompress[false]
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(16, reader.size());
        for (int i = 0; i < 16; i++) {
            ASSERT_TRUE(!reader.read(i)->compress());
            if (i < 8) {
                ASSERT_TRUE(reader.read(i)->merged());
                ASSERT_EQ(32767, reader.read(i)->uint16Payload()) << i;
                ASSERT_EQ(data.size() * 4 + 34, reader.read(i)->data()->size());

            } else if (i == 15) {
                ASSERT_TRUE(!reader.read(i)->merged());
                ASSERT_EQ(61440, reader.read(i)->uint16Payload()) << i;
                ASSERT_EQ(data, reader.read(i)->data()->str());

            } else {
                ASSERT_TRUE(reader.read(i)->merged());
                ASSERT_EQ(65535, reader.read(i)->uint16Payload()) << i;
                ASSERT_EQ(data.size() * 4 + 34, reader.read(i)->data()->size());
            }
        }
    }
    { // client support merge [true] decompress [true]
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.set_candecompressmsg(true);
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(16, reader.size());
        for (int i = 0; i < 16; i++) {
            ASSERT_TRUE(reader.read(i)->compress()) << i;
            if (i < 8) {
                ASSERT_TRUE(reader.read(i)->merged());
                ASSERT_EQ(32767, reader.read(i)->uint16Payload()) << i;
                ASSERT_TRUE(data.size() * 4 + 34 != reader.read(i)->data()->size());
            } else if (i == 15) {
                ASSERT_TRUE(!reader.read(i)->merged());
                ASSERT_EQ(61440, reader.read(i)->uint16Payload()) << i;
                ASSERT_TRUE(data != reader.read(i)->data()->str());
            } else {
                ASSERT_TRUE(reader.read(i)->merged());
                ASSERT_EQ(65535, reader.read(i)->uint16Payload()) << i;
                ASSERT_TRUE(data.size() * 4 + 34 != reader.read(i)->data()->size());
            }
        }
    }

    { // client support merge [false] with meta filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(0);
        request.set_count(100);
        Filter filter;
        filter.set_from(27648); // 27
        filter.set_to(43008);   // 42
        *request.mutable_filter() = filter;
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(16, reader.size());
        for (int i = 0; i < 16; i++) {
            ASSERT_TRUE(!reader.read(i)->merged()) << i;
            ASSERT_TRUE(!reader.read(i)->compress()) << i;
            ASSERT_EQ(i + 27, reader.read(i)->uint8MaskPayload());
            ASSERT_EQ((i + 27) * 1024, reader.read(i)->uint16Payload());
            ASSERT_EQ(data, reader.read(i)->data()->str());
        }
    }
    { // client support merge [false] with meta filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(0);
        request.set_count(100);
        Filter filter;
        filter.set_from(27648); // 27
        filter.set_to(65535);
        *request.mutable_filter() = filter;
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(34, reader.size());
        for (int i = 0; i < 34; i++) {
            ASSERT_TRUE(!reader.read(i)->merged()) << i;
            ASSERT_TRUE(!reader.read(i)->compress()) << i;
            ASSERT_EQ(i + 27, reader.read(i)->uint8MaskPayload());
            ASSERT_EQ((i + 27) * 1024, reader.read(i)->uint16Payload());
            ASSERT_EQ(data, reader.read(i)->data()->str());
        }
    }
    { // client support merge [false] with field filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(0);
        request.set_count(100);
        request.set_fieldfilterdesc("field_b IN 1");
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(61, reader.size());
        for (int i = 0; i < 61; i++) {
            ASSERT_TRUE(!reader.read(i)->merged()) << i;
            ASSERT_TRUE(!reader.read(i)->compress()) << i;
            ASSERT_EQ(i, reader.read(i)->uint8MaskPayload());
            ASSERT_EQ(i * 1024, reader.read(i)->uint16Payload());
            ASSERT_EQ(data, reader.read(i)->data()->str());
        }
    }
    { // client support merge [false] with field filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(0);
        request.set_count(100);
        request.set_fieldfilterdesc("field_b IN 0");
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(0, reader.size());
    }
    { // client support merge [true] with meta filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.set_startid(0);
        request.set_count(100);
        Filter filter;
        filter.set_from(27648); // 27
        filter.set_to(43008);   // 42
        *request.mutable_filter() = filter;
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(15, reader.size());
        for (int i = 0; i < 15; i++) {
            ASSERT_TRUE(reader.read(i)->merged()) << i;
            ASSERT_TRUE(!reader.read(i)->compress()) << i;
            if (i < 8) {
                ASSERT_EQ(32767, reader.read(i)->uint16Payload()) << i;
            } else {
                ASSERT_EQ(65535, reader.read(i)->uint16Payload()) << i;
            }
            ASSERT_EQ(data.size() * 4 + 34, reader.read(i)->data()->size());
        }
    }
    { // client support merge [true] with meta filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(0);
        request.set_count(100);
        Filter filter;
        filter.set_from(43008); // 27
        filter.set_to(43008);   // 42
        *request.mutable_filter() = filter;
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(7, reader.size());
        for (int i = 0; i < 7; i++) {
            ASSERT_TRUE(reader.read(i)->merged()) << i;
            ASSERT_TRUE(!reader.read(i)->compress()) << i;
            ASSERT_EQ(65535, reader.read(i)->uint16Payload()) << i;
            ASSERT_EQ(data.size() * 4 + 34, reader.read(i)->data()->size());
        }
    }
    { // client support merge [true] with filed filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_fieldfilterdesc("field_b IN 0");
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(15, reader.size());
        for (int i = 0; i < 15; i++) {
            ASSERT_TRUE(reader.read(i)->merged()) << i;
            ASSERT_TRUE(!reader.read(i)->compress()) << i;
        }
    }
    { // client support merge [true] compress [true] with meta filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_candecompressmsg(true);
        request.set_startid(0);
        request.set_count(100);
        Filter filter;
        filter.set_from(43008); // 27
        filter.set_to(43008);   // 42
        *request.mutable_filter() = filter;
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(7, reader.size());
        for (int i = 0; i < 7; i++) {
            ASSERT_TRUE(reader.read(i)->merged()) << i;
            ASSERT_TRUE(reader.read(i)->compress()) << i;
            ASSERT_EQ(65535, reader.read(i)->uint16Payload()) << i;
            ASSERT_TRUE(data.size() * 4 + 34 > reader.read(i)->data()->size());
        }
    }
    { // client support merge [true] compress [true] with filed filter
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportmergemsg(true);
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_candecompressmsg(true);
        request.set_fieldfilterdesc("field_b IN 0");
        request.set_startid(0);
        request.set_count(100);
        MessageResponse response;
        ErrorCode ec = brokerPart->getMessage(&request, &response);
        ASSERT_EQ(ERROR_NONE, ec);
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(15, reader.size());
        for (int i = 0; i < 15; i++) {
            ASSERT_TRUE(reader.read(i)->merged()) << i;
            ASSERT_TRUE(reader.read(i)->compress()) << i;
        }
    }
}

TEST_F(BrokerPartitionTest, testGetMessage) {
    auto taskInfo = makeTaskInfo("topic_0", TOPIC_MODE_MEMORY_ONLY, 128, 128);
    taskInfo.set_sealed(true);
    BrokerPartitionPtr brokerPart(new BrokerPartition(taskInfo));
    ConsumptionRequest request;
    request.set_startid(0);
    request.set_count(100);
    MessageResponse response;
    MockMessageBrain *msgBrain = new MockMessageBrain;
    brokerPart->_messageBrain = msgBrain;
    brokerPart->_rangeUtil = new RangeUtil(1);
    auto mockGetMessage = [](const protocol::ConsumptionRequest *request,
                             protocol::MessageResponse *response,
                             util::TimeoutChecker *timeoutChecker,
                             const std::string *srcIpPort,
                             monitor::ReadMetricsCollector &collector) {
        response->set_maxtimestamp(100);
        response->set_nexttimestamp(101);
        response->set_maxmsgid(200);
        response->set_nextmsgid(201);
        response->set_totalmsgcount(0);
        return ERROR_NONE;
    };
    EXPECT_CALL(*msgBrain, getMessage(_, _, _, _, _)).WillOnce(Invoke(mockGetMessage));
    ErrorCode ec = brokerPart->getMessage(&request, &response);
    ASSERT_EQ(ERROR_SEALED_TOPIC_READ_FINISH, ec);

    EXPECT_CALL(*msgBrain, getMessage(_, _, _, _, _)).WillOnce(Return(ERROR_BROKER_BUSY));
    ec = brokerPart->getMessage(&request, &response);
    ASSERT_EQ(ERROR_BROKER_BUSY, ec);

    EXPECT_CALL(*msgBrain, getMessage(_, _, _, _, _)).WillOnce(Return(ERROR_BROKER_SOME_MESSAGE_LOST));
    ec = brokerPart->getMessage(&request, &response);
    ASSERT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);

    brokerPart->_taskStatus->_taskStatus.mutable_taskinfo()->set_sealed(false);
    EXPECT_CALL(*msgBrain, getMessage(_, _, _, _, _)).WillOnce(Return(ERROR_BROKER_NO_DATA));
    ec = brokerPart->getMessage(&request, &response);
    ASSERT_EQ(ERROR_BROKER_NO_DATA, ec);

    brokerPart->_taskStatus->_taskStatus.mutable_taskinfo()->set_sealed(true);
    EXPECT_CALL(*msgBrain, getMessage(_, _, _, _, _)).WillOnce(Return(ERROR_BROKER_NO_DATA));
    ec = brokerPart->getMessage(&request, &response);
    ASSERT_EQ(ERROR_SEALED_TOPIC_READ_FINISH, ec);

    EXPECT_CALL(*msgBrain, getMessage(_, _, _, _, _)).WillOnce(Return(ERROR_BROKER_SESSION_CHANGED));
    ec = brokerPart->getMessage(&request, &response);
    ASSERT_EQ(ERROR_BROKER_SESSION_CHANGED, ec);
}

TEST_F(BrokerPartitionTest, testSendMessageVersionControl) {
    auto taskInfo = makeTaskInfo("topic1_1", TOPIC_MODE_MEMORY_ONLY, 128, 128);
    taskInfo.set_versioncontrol(true);
    zookeeper::ZkServer *zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << zkServer->port();
    cm_basic::ZkWrapper *zkWrapper = new cm_basic::ZkWrapper(oss.str(), 1000);
    ASSERT_TRUE(zkWrapper->open());
    _config.zkRoot = "zfs://" + oss.str() + "/testSendMessageVersionControl";
    ZkDataAccessorPtr zkDataAccessor(new ZkDataAccessor());
    ASSERT_TRUE(zkDataAccessor->init(zkWrapper, _config.zkRoot));
    auto brokerPart = std::make_unique<BrokerPartition>(taskInfo, nullptr, zkDataAccessor);
    ASSERT_TRUE(brokerPart->init(_config, _centerPool.get(), _fileCachePool.get(), _permissionCenter.get()));
    ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, brokerPart->_messageBrain->getTopicMode());
    ASSERT_TRUE(brokerPart->_writerVersionController != nullptr);

    ProductionRequest request;
    Message *message = request.add_msgs();
    message->set_data("aaa");
    MessageResponse response;
    { // 1. version control, but request not set write name
        FakeClosure done;
        ProductionLogClosure *closure = new ProductionLogClosure(&request, &response, &done, "sendMessage");
        ASSERT_EQ(ERROR_BROKER_WRITE_VERSION_INVALID, brokerPart->sendMessage(closure));
    }
    { // 2. version control, OK, update version
        request.set_writername("processor_1_100");
        request.set_majorversion(100);
        request.set_minorversion(1000);
        FakeClosure done;
        ProductionLogClosure *closure = new ProductionLogClosure(&request, &response, &done, "sendMessage");
        ASSERT_EQ(ERROR_NONE, brokerPart->sendMessage(closure));
    }
    { // 3. version control, but request write version smaller than manager's version
        request.set_minorversion(999);
        FakeClosure done;
        ProductionLogClosure *closure = new ProductionLogClosure(&request, &response, &done, "sendMessage");
        ASSERT_EQ(ERROR_BROKER_WRITE_VERSION_INVALID, brokerPart->sendMessage(closure));
    }
    DELETE_AND_SET_NULL(zkWrapper);
}

TEST_F(BrokerPartitionTest, testStealTimeoutPolling) {
    TaskInfo taskInfo;
    *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic_0");
    ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
    BrokerPartitionPtr brokerPart(new BrokerPartition(threadSafeTaskStatus));
    brokerPart->_pollingMinStartId = 100;
    brokerPart->_pollingMaxMsgId = 200;
    protocol::ConsumptionRequest request;
    protocol::MessageResponse response;
    request.set_startid(300);
    FakeClosure done;
    util::ConsumptionLogClosurePtr clouse(new util::ConsumptionLogClosure(&request, &response, &done, "aaa"));
    brokerPart->_pollingQueue.emplace_back(50, std::move(clouse));
    BrokerPartition::ReadRequestQueue reqQueue;
    int64_t currentTime = 10;
    int64_t maxHoldTime = 20;
    brokerPart->stealTimeoutPolling(false, currentTime, maxHoldTime, reqQueue);
    EXPECT_EQ(1, reqQueue.size());
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), brokerPart->_pollingMinStartId);
    EXPECT_EQ(0, brokerPart->_pollingQueue.size());
}

TEST_F(BrokerPartitionTest, testStealTimeoutPollingSkip) {
    TaskInfo taskInfo;
    *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic_0");
    ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
    BrokerPartitionPtr brokerPart(new BrokerPartition(threadSafeTaskStatus));
    brokerPart->_pollingMinStartId = 100;
    brokerPart->_pollingMaxMsgId = 200;
    protocol::ConsumptionRequest request;
    protocol::MessageResponse response;
    request.set_startid(300);
    FakeClosure done;
    util::ConsumptionLogClosurePtr clouse(new util::ConsumptionLogClosure(&request, &response, &done, "aaa"));
    brokerPart->_pollingQueue.emplace_back(20, std::move(clouse));
    BrokerPartition::ReadRequestQueue reqQueue;
    int64_t currentTime = 10;
    int64_t maxHoldTime = 20;
    brokerPart->stealTimeoutPolling(false, currentTime, maxHoldTime, reqQueue);
    EXPECT_EQ(0, reqQueue.size());
    EXPECT_EQ(300, brokerPart->_pollingMinStartId);
    EXPECT_EQ(1, brokerPart->_pollingQueue.size());
    brokerPart->_pollingQueue.clear();
}

TEST_F(BrokerPartitionTest, testStealNeedActivePollingEmpty) {
    TaskInfo taskInfo;
    *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic_0");
    ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
    BrokerPartitionPtr brokerPart(new BrokerPartition(threadSafeTaskStatus));
    brokerPart->_pollingMinStartId = 100;
    brokerPart->_pollingMaxMsgId = 200;
    BrokerPartition::ReadRequestQueue reqQueue;
    int64_t maxMsgId = 1;
    uint64_t compressPayload = 0;
    brokerPart->stealNeedActivePolling(reqQueue, compressPayload, maxMsgId);
    ASSERT_TRUE(reqQueue.empty());
    EXPECT_EQ(0, brokerPart->_pollingQueue.size());
    brokerPart->_pollingQueue.clear();
}

TEST_F(BrokerPartitionTest, testStealNeedActivePollingSkip) {
    TaskInfo taskInfo;
    *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic_0");
    ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
    BrokerPartitionPtr brokerPart(new BrokerPartition(threadSafeTaskStatus));
    brokerPart->_pollingMinStartId = 100;
    brokerPart->_pollingMaxMsgId = 200;
    protocol::ConsumptionRequest request;
    protocol::MessageResponse response;
    request.set_startid(300);
    FakeClosure done;
    util::ConsumptionLogClosurePtr clouse(new util::ConsumptionLogClosure(&request, &response, &done, "aaa"));
    brokerPart->_pollingQueue.emplace_back(0, std::move(clouse));
    BrokerPartition::ReadRequestQueue reqQueue;
    int64_t maxMsgId = 100;
    uint64_t compressPayload = 0;
    brokerPart->stealNeedActivePolling(reqQueue, compressPayload, maxMsgId);
    EXPECT_EQ(0, reqQueue.size());
    EXPECT_EQ(300, brokerPart->_pollingMinStartId);
    EXPECT_EQ(1, brokerPart->_pollingQueue.size());
    brokerPart->_pollingQueue.clear();
}

TEST_F(BrokerPartitionTest, testStealNeedActivePollingOne) {
    TaskInfo taskInfo;
    *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic_0");
    ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
    BrokerPartitionPtr brokerPart(new BrokerPartition(threadSafeTaskStatus));
    brokerPart->_pollingMinStartId = 100;
    brokerPart->_pollingMaxMsgId = 200;
    protocol::ConsumptionRequest request;
    protocol::MessageResponse response;
    FakeClosure done;
    util::ConsumptionLogClosurePtr clouse(new util::ConsumptionLogClosure(&request, &response, &done, "aaa"));
    brokerPart->_pollingQueue.emplace_back(0, std::move(clouse));
    BrokerPartition::ReadRequestQueue reqQueue;
    int64_t maxMsgId = 100;
    uint64_t compressPayload = 0;
    brokerPart->stealNeedActivePolling(reqQueue, compressPayload, maxMsgId);
    EXPECT_EQ(1, reqQueue.size());
    brokerPart->_pollingQueue.clear();
}

TEST_F(BrokerPartitionTest, testStealNeedActivePollingCheckMaxMsgId) {
    { // maxMsgId != -1 && maxMsgId >= _pollingMinStartId
        TaskInfo taskInfo;
        *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic_0");
        ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
        BrokerPartitionPtr brokerPart(new BrokerPartition(threadSafeTaskStatus));
        brokerPart->_pollingMinStartId = 100;
        brokerPart->_pollingMaxMsgId = 200;
        protocol::ConsumptionRequest request;
        protocol::MessageResponse response;
        FakeClosure done;
        util::ConsumptionLogClosurePtr clouse(new util::ConsumptionLogClosure(&request, &response, &done, "aaa"));
        brokerPart->_pollingQueue.emplace_back(0, std::move(clouse));
        BrokerPartition::ReadRequestQueue reqQueue;
        int64_t maxMsgId = 100;
        uint64_t compressPayload = 0;
        brokerPart->stealNeedActivePolling(reqQueue, compressPayload, maxMsgId);
        EXPECT_EQ(1, reqQueue.size());
        brokerPart->_pollingQueue.clear();
    }

    { // maxMsgId != -1 && maxMsgId < _pollingMinStartId
        TaskInfo taskInfo;
        *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic_0");
        ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
        BrokerPartitionPtr brokerPart(new BrokerPartition(threadSafeTaskStatus));
        brokerPart->_pollingMinStartId = 100;
        brokerPart->_pollingMaxMsgId = 200;
        protocol::ConsumptionRequest request;
        protocol::MessageResponse response;
        FakeClosure done;
        util::ConsumptionLogClosurePtr clouse(new util::ConsumptionLogClosure(&request, &response, &done, "aaa"));
        brokerPart->_pollingQueue.emplace_back(0, std::move(clouse));
        BrokerPartition::ReadRequestQueue reqQueue;
        int64_t maxMsgId = 99;
        uint64_t compressPayload = 0;
        brokerPart->stealNeedActivePolling(reqQueue, compressPayload, maxMsgId);
        EXPECT_EQ(0, reqQueue.size());
        brokerPart->_pollingQueue.clear();
    }

    { // maxMsgId == -1
        TaskInfo taskInfo;
        *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic_0");
        ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
        BrokerPartitionPtr brokerPart(new BrokerPartition(threadSafeTaskStatus));
        brokerPart->_pollingMinStartId = 100;
        brokerPart->_pollingMaxMsgId = 200;
        protocol::ConsumptionRequest request;
        protocol::MessageResponse response;
        FakeClosure done;
        util::ConsumptionLogClosurePtr clouse(new util::ConsumptionLogClosure(&request, &response, &done, "aaa"));
        brokerPart->_pollingQueue.emplace_back(0, std::move(clouse));
        BrokerPartition::ReadRequestQueue reqQueue;
        int64_t maxMsgId = -1;
        uint64_t compressPayload = 0;
        brokerPart->stealNeedActivePolling(reqQueue, compressPayload, maxMsgId);
        EXPECT_EQ(1, reqQueue.size());
        brokerPart->_pollingQueue.clear();
    }
}

TEST_F(BrokerPartitionTest, testStealAllPolling) {
    TaskInfo taskInfo;
    *(taskInfo.mutable_partitionid()) = MessageCreator::createPartitionId("topic_0");
    ThreadSafeTaskStatusPtr threadSafeTaskStatus(new ThreadSafeTaskStatus(taskInfo));
    BrokerPartitionPtr brokerPart(new BrokerPartition(threadSafeTaskStatus));
    brokerPart->_pollingMinStartId = 100;
    brokerPart->_pollingMaxMsgId = 200;
    protocol::ConsumptionRequest request;
    protocol::MessageResponse response;
    FakeClosure done;
    util::ConsumptionLogClosurePtr clouse(new util::ConsumptionLogClosure(&request, &response, &done, "aaa"));
    brokerPart->_pollingQueue.emplace_back(0, std::move(clouse));
    BrokerPartition::ReadRequestQueue reqQueue;
    brokerPart->stealAllPolling(reqQueue);
    EXPECT_EQ(1, reqQueue.size());
    EXPECT_EQ(0, brokerPart->_pollingQueue.size());
}

ErrorCode BrokerPartitionTest::getMessage(BrokerPartition &partition, uint64_t startId, uint32_t count) {
    ConsumptionRequest request;
    request.set_startid(startId);
    request.set_count(count);
    MessageResponse response;
    return partition.getMessage(&request, &response);
}

ErrorCode BrokerPartitionTest::sendMessage(BrokerPartition &partition, uint32_t count) {
    ProductionRequest request;
    string data = "aaa";
    for (uint32_t i = 0; i < count; ++i) {
        Message *message = request.add_msgs();
        message->set_data(data.c_str());
    }
    MessageResponse response;
    FakeClosure done;
    ProductionLogClosure *closure = new ProductionLogClosure(&request, &response, &done, "sendMessage");
    ErrorCode ec = partition.sendMessage(closure);
    assert(done.getDoneStatus());
    return ec;
}

ErrorCode BrokerPartitionTest::sendMessage(BrokerPartition &partition,
                                           uint32_t count,
                                           const string &data,
                                           bool compressMsg,
                                           bool compressRequest,
                                           bool errorMsgCompress,
                                           bool mergeMessage) {
    ProductionRequest request;
    vector<Message> msgVec;
    for (uint32_t i = 0; i < count; ++i) {
        Message message;
        message.set_data(data);
        message.set_uint16payload(i * 1024 % 65536);
        message.set_uint8maskpayload(i % 256);
        if (errorMsgCompress) {
            message.set_compress(!compressMsg);
        }
        msgVec.push_back(message);
    }
    if (mergeMessage) {
        vector<Message> mergeVec;
        RangeUtil rangUtil(1, 2);
        MessageUtil::mergeMessage(msgVec, &rangUtil, 4, mergeVec);
        msgVec = mergeVec;
    }
    for (size_t i = 0; i < msgVec.size(); i++) {
        ZlibCompressor compressor;
        if (msgVec[i].merged() && compressMsg) {
            string data;
            if (MessageCompressor::compressMergedMessage(
                    &compressor, 10, msgVec[i].data().c_str(), msgVec[i].data().size(), data)) {
                msgVec[i].set_data(data);
                msgVec[i].set_compress(true);
            }
        }
        *request.add_msgs() = msgVec[i];
    }

    if (compressMsg) {
        MessageCompressor::compressProductionMessage(&request, 0);
    }
    if (compressRequest) {
        MessageCompressor::compressProductionRequest(&request);
    }
    MessageResponse response;

    FakeClosure done;
    ProductionLogClosure *closure = new ProductionLogClosure(&request, &response, &done, "sendMessage");
    ErrorCode ec = partition.sendMessage(closure);
    assert(done.getDoneStatus());
    return ec;
}

ErrorCode BrokerPartitionTest::getMaxMessageId(BrokerPartition &partition, int64_t &msgId) {
    MessageIdRequest request;
    MessageIdResponse response;

    ErrorCode ec = partition.getMaxMessageId(&request, &response);
    if (ec == ERROR_NONE) {
        msgId = response.msgid();
    }
    return ec;
}

ErrorCode BrokerPartitionTest::getMinMessageIdByTime(BrokerPartition &partition, int64_t timestamp, int64_t &msgId) {
    MessageIdRequest request;
    request.set_timestamp(timestamp);
    MessageIdResponse response;

    ErrorCode ec = partition.getMinMessageIdByTime(&request, &response);
    if (ec == ERROR_NONE) {
        msgId = response.msgid();
    }
    return ec;
}

} // namespace service
} // namespace swift
