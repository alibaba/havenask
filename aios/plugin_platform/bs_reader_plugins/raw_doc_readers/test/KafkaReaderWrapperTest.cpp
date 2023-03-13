#include <unittest/unittest.h>
#include <autil/StringUtil.h>
#include "raw_doc_readers/KafkaReaderWrapper.h"
#include "MockKafkaConsumer.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service;

namespace pluginplatform {
namespace reader_plugins {

class MockKafkaReaderWrapper : public KafkaReaderWrapper {
public:
    MOCK_METHOD2(createConsumer, RdKafka::Consumer* (
                    std::shared_ptr<RdKafka::Conf> &conf, std::string &errStr));
    MOCK_METHOD4(createTopic, RdKafka::Topic* (
            std::shared_ptr<RdKafka::Consumer> &consumer, std::string &topicName,
            std::shared_ptr<RdKafka::Conf> &conf, std::string &errStr));
};

class KafkaReaderWrapperTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
public:
    void printMessage(std::shared_ptr<RdKafka::Message> &message, bool force=false) {
        if (!force) {
            return;
        }
        std::cout << "----" << std::endl;
        std::cout << "messge.pointer:" << message.get() << std::endl;
        if (!message) {
            return;
        }
        std::cout << "messge.key:" << *(message->key()) << std::endl;
        std::cout << "messge.payload:"
                  << static_cast<const char *>(message->payload()) << std::endl;
        std::cout << "messge.timestamp:" << message->timestamp().timestamp << std::endl;
    }

    bool getAllMessages(std::vector<std::shared_ptr<RdKafka::Message>> &messages) {
        KafkaReaderParam param;
        param.topicName = "quickstart-events";
        param.range.set_from(0);
        param.range.set_to(65535);

        KeyValueMap kvMap;
        kvMap["bootstrap.servers"] = "localhost:9092";

        _kafkaReaderWrapper.init(param, kvMap);

        int64_t timestamp;
        std::shared_ptr<RdKafka::Message> message;
        RdKafka::KafkaClientErrorCode ec(RdKafka::ERR_NO_ERROR);
        while (true) {
            ec = _kafkaReaderWrapper.read(timestamp, message, 3000);
            if (RdKafka::ERR_NO_ERROR != ec) {
                if(RdKafka::ERR__PARTITION_EOF == ec) {
                    std::cout << "get all message eof" << std::endl;
                    return true;
                }
                std::cout << RdKafka::err2str(ec) << std::endl;
                return false;
            }
            messages.push_back(message);
        }
        return true;
    }
    bool prepareMessages() {
        if (!getAllMessages(_messages)) {
            return false;
        }
        auto cmp = [](const std::shared_ptr<RdKafka::Message> &lhs,
                      const std::shared_ptr<RdKafka::Message> &rhs) {
                       return lhs->timestamp().timestamp < rhs->timestamp().timestamp;
                   };
        stable_sort(_messages.begin(), _messages.end(), cmp);
        // std::cout << "_messages.size:" << _messages.size() << std::endl;
        for (size_t i = 0; i < _messages.size(); ++i) {
            auto &msg = _messages[i];
            // std::cout << "_messages[" << i << "]:" << std::endl;
            printMessage(msg);
        }
        return true;
    }
private:
    KafkaReaderWrapper _kafkaReaderWrapper;
    // note: delete messages before destroy kafka reader wrapper
    std::vector<std::shared_ptr<RdKafka::Message>> _messages;
};

void KafkaReaderWrapperTest::setUp() {
    // prepareMessages();
}

void KafkaReaderWrapperTest::tearDown() {
}

TEST_F(KafkaReaderWrapperTest, testSimpleProcess) {
    KafkaReaderParam param;
    param.topicName = "quickstart-events";
    param.range.set_from(0);
    param.range.set_to(65535);
    KeyValueMap kvMap;
    kvMap["bootstrap.servers"] = "localhost:9092";

    // mock init info
    MockKafkaReaderWrapper kafkaReaderWrapper;
    MockKafkaConsumer* mockConsumer = new MockKafkaConsumer;
    MockTopic* mockTopic = new MockTopic;
    EXPECT_CALL(kafkaReaderWrapper, createConsumer(_, _))
        .WillRepeatedly(Return(mockConsumer));
    EXPECT_CALL(kafkaReaderWrapper, createTopic(_, _, _, _))
        .WillRepeatedly(Return(mockTopic));

    MockMetadata* mockMetadata =  new MockMetadata;
    std::shared_ptr<MockTopicMetadata> topicMetaData(new MockTopicMetadata);
    RdKafka::TopicMetadata::PartitionMetadataVector partMetaVec{
        (RdKafka::PartitionMetadata*)1, (RdKafka::PartitionMetadata*)2};
    const RdKafka::Metadata::TopicMetadataVector topicMetaVec{ topicMetaData.get() };
    EXPECT_CALL(*mockMetadata, topics())
        .WillRepeatedly(Return(&topicMetaVec));
    EXPECT_CALL(*topicMetaData, partitions())
        .WillRepeatedly(Return(&partMetaVec));
    auto metaFunc = [&](bool, const RdKafka::Topic*, RdKafka::Metadata**pmeta, int) {
                        *pmeta = mockMetadata;
                        return RdKafka::ERR_NO_ERROR;
                    };
    EXPECT_CALL(*mockConsumer, metadata(_, _, _, _))
        .WillRepeatedly(metaFunc);
    EXPECT_CALL(*mockConsumer, start(_, _, _))
        .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
    EXPECT_CALL(*mockConsumer, stop(_, _))
        .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
    EXPECT_CALL(*mockConsumer, seek(_, _, _, _))
        .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
    EXPECT_CALL(*mockConsumer, poll(_))
        .WillRepeatedly(Return(0));
    ASSERT_TRUE(kafkaReaderWrapper.init(param, kvMap));

    // mock consume message
    vector<MockMessage *> mockRetMessages;
    size_t totalMessageCount = 7;
    std::string key("test_key");
    std::string payload("test_payload");
    for (size_t i = 0; i < totalMessageCount; ++i) {
        MockMessage *mockRetMessage = new MockMessage;
        mockRetMessages.push_back(mockRetMessage);
        EXPECT_CALL(*mockRetMessage, err())
            .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
        EXPECT_CALL(*mockRetMessage, key())
            .WillRepeatedly(Return(&key));
        EXPECT_CALL(*mockRetMessage, payload())
            .WillRepeatedly(Return((void*)payload.c_str()));
        RdKafka::MessageTimestamp ts{
            RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, i};
        EXPECT_CALL(*mockRetMessage, timestamp())
            .WillRepeatedly(Return(ts));
    }
    auto mockConsumeFunc =
        [&](RdKafka::Topic*, int32_t, int) {
            static size_t cnt = 0;
            assert(cnt <= mockRetMessages.size());
            if (cnt < totalMessageCount) {
                return mockRetMessages[cnt++];
            } else {
                MockMessage *mockRetMessage = new MockMessage;
                EXPECT_CALL(*mockRetMessage, err())
                    .WillRepeatedly(Return(RdKafka::ERR__PARTITION_EOF));
                    return mockRetMessage;
            }
        };
    EXPECT_CALL(*mockConsumer, consume(_, _, _)).WillRepeatedly(mockConsumeFunc);
    int64_t timestamp;
    std::shared_ptr<RdKafka::Message> message;
    RdKafka::KafkaClientErrorCode ec(RdKafka::ERR_NO_ERROR);
    for (size_t i = 0; i < totalMessageCount; ++i) {
        ec = kafkaReaderWrapper.read(timestamp, message, 3000);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, ec);
        printMessage(message);
    }
    // kafka eof
    ec = kafkaReaderWrapper.read(timestamp, message, 3000);
    EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
    // wait no more message timeout eof
    ec = kafkaReaderWrapper.read(timestamp, message, 3000);
    EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
    // wait no more message timeout eof
    ec = kafkaReaderWrapper.read(timestamp, message, 3000);
    EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
}

TEST_F(KafkaReaderWrapperTest, testSetTimestampLimit) {
    KafkaReaderParam param;
    param.topicName = "quickstart-events";
    param.range.set_from(0);
    param.range.set_to(65535);
    KeyValueMap kvMap;
    kvMap["bootstrap.servers"] = "localhost:9092";

    // mock init info
    MockKafkaReaderWrapper kafkaReaderWrapper;
    MockKafkaConsumer* mockConsumer = new MockKafkaConsumer;
    MockTopic* mockTopic = new MockTopic;
    EXPECT_CALL(kafkaReaderWrapper, createConsumer(_, _))
        .WillRepeatedly(Return(mockConsumer));
    EXPECT_CALL(kafkaReaderWrapper, createTopic(_, _, _, _))
        .WillRepeatedly(Return(mockTopic));

    MockMetadata* mockMetadata =  new MockMetadata;
    std::shared_ptr<MockTopicMetadata> topicMetaData(new MockTopicMetadata);
    RdKafka::TopicMetadata::PartitionMetadataVector partMetaVec{
        (RdKafka::PartitionMetadata*)1, (RdKafka::PartitionMetadata*)2};
    const RdKafka::Metadata::TopicMetadataVector topicMetaVec{ topicMetaData.get() };
    EXPECT_CALL(*mockMetadata, topics())
        .WillRepeatedly(Return(&topicMetaVec));
    EXPECT_CALL(*topicMetaData, partitions())
        .WillRepeatedly(Return(&partMetaVec));
    auto metaFunc = [&](bool, const RdKafka::Topic*, RdKafka::Metadata**pmeta, int) {
                        *pmeta = mockMetadata;
                        return RdKafka::ERR_NO_ERROR;
                    };
    EXPECT_CALL(*mockConsumer, metadata(_, _, _, _))
        .WillRepeatedly(metaFunc);
    EXPECT_CALL(*mockConsumer, start(_, _, _))
        .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
    EXPECT_CALL(*mockConsumer, stop(_, _))
        .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
    EXPECT_CALL(*mockConsumer, seek(_, _, _, _))
        .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
    EXPECT_CALL(*mockConsumer, poll(_))
        .WillRepeatedly(Return(0));
    ASSERT_TRUE(kafkaReaderWrapper.init(param, kvMap));

    // mock consume message
    vector<MockMessage *> mockRetMessages;
    size_t totalMessageCount = 7;
    std::string key("test_key");
    std::string payload("test_payload");
    for (size_t i = 0; i < totalMessageCount; ++i) {
        MockMessage *mockRetMessage = new MockMessage;
        mockRetMessages.push_back(mockRetMessage);
        EXPECT_CALL(*mockRetMessage, err())
            .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
        EXPECT_CALL(*mockRetMessage, key())
            .WillRepeatedly(Return(&key));
        EXPECT_CALL(*mockRetMessage, payload())
            .WillRepeatedly(Return((void*)payload.c_str()));
        RdKafka::MessageTimestamp ts{
            RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, i};
        EXPECT_CALL(*mockRetMessage, timestamp())
            .WillRepeatedly(Return(ts));
    }
    auto mockConsumeFunc =
        [&](RdKafka::Topic*, int32_t, int) {
            static size_t cnt = 0;
            assert(cnt <= mockRetMessages.size());
            if (cnt < totalMessageCount) {
                return mockRetMessages[cnt++];
            } else {
                MockMessage *mockRetMessage = new MockMessage;
                EXPECT_CALL(*mockRetMessage, err())
                    .WillRepeatedly(Return(RdKafka::ERR__PARTITION_EOF));
                    return mockRetMessage;
            }
        };
    EXPECT_CALL(*mockConsumer, consume(_, _, _)).WillRepeatedly(mockConsumeFunc);
    int64_t timestamp;
    std::shared_ptr<RdKafka::Message> message;
    RdKafka::KafkaClientErrorCode ec(RdKafka::ERR_NO_ERROR);
    int64_t timestampLimit = 3;
    int64_t acceptTimestamp;
    size_t msgCount = 0;
    kafkaReaderWrapper.setTimestampLimit(timestampLimit, acceptTimestamp);
    while (true) {
        ec = kafkaReaderWrapper.read(timestamp, message, 3000);
        if (RdKafka::ERR_NO_ERROR != ec) {
            break;
        }
        msgCount++;
        EXPECT_TRUE(message->timestamp().timestamp <= timestampLimit);
    }
    EXPECT_EQ(4, msgCount);
    // kafka eof
    ec = kafkaReaderWrapper.read(timestamp, message, 3000);
    EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
}

TEST_F(KafkaReaderWrapperTest, testSeekByTimestamp) {
    KafkaReaderParam param;
    param.topicName = "quickstart-events";
    param.range.set_from(0);
    param.range.set_to(65535);
    KeyValueMap kvMap;
    kvMap["bootstrap.servers"] = "localhost:9092";

    // mock init info
    MockKafkaReaderWrapper kafkaReaderWrapper;
    MockKafkaConsumer* mockConsumer = new MockKafkaConsumer;
    MockTopic* mockTopic = new MockTopic;
    EXPECT_CALL(kafkaReaderWrapper, createConsumer(_, _))
        .WillRepeatedly(Return(mockConsumer));
    EXPECT_CALL(kafkaReaderWrapper, createTopic(_, _, _, _))
        .WillRepeatedly(Return(mockTopic));
    EXPECT_CALL(*mockTopic, name())
        .WillRepeatedly(Return(param.topicName));

    MockMetadata* mockMetadata =  new MockMetadata;
    std::shared_ptr<MockTopicMetadata> topicMetaData(new MockTopicMetadata);
    RdKafka::TopicMetadata::PartitionMetadataVector partMetaVec{
        (RdKafka::PartitionMetadata*)1, (RdKafka::PartitionMetadata*)2};
    const RdKafka::Metadata::TopicMetadataVector topicMetaVec{ topicMetaData.get() };
    EXPECT_CALL(*mockMetadata, topics())
        .WillRepeatedly(Return(&topicMetaVec));
    EXPECT_CALL(*topicMetaData, partitions())
        .WillRepeatedly(Return(&partMetaVec));
    auto metaFunc = [&](bool, const RdKafka::Topic*, RdKafka::Metadata**pmeta, int) {
                        *pmeta = mockMetadata;
                        return RdKafka::ERR_NO_ERROR;
                    };
    EXPECT_CALL(*mockConsumer, metadata(_, _, _, _))
        .WillRepeatedly(metaFunc);
    EXPECT_CALL(*mockConsumer, start(_, _, _))
        .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
    EXPECT_CALL(*mockConsumer, stop(_, _))
        .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
    EXPECT_CALL(*mockConsumer, offsetsForTimes(Matcher<std::vector<RdKafka::TopicPartition*>&>(_), _))
        .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));

    size_t msgCur = 0;
    auto seekFunc = [&](RdKafka::Topic*, int32_t, int64_t offset, int) {
                        msgCur = offset;
                        return RdKafka::ERR_NO_ERROR;
                    };
    EXPECT_CALL(*mockConsumer, seek(_, _, _, _))
        .WillRepeatedly(seekFunc);
    EXPECT_CALL(*mockConsumer, poll(_))
        .WillRepeatedly(Return(0));
    ASSERT_TRUE(kafkaReaderWrapper.init(param, kvMap));

    // mock consume message
    size_t totalMessageCount = 7;
    std::string key("test_key");
    std::string payload("test_payload");
    auto mockConsumeFunc =
        [&](RdKafka::Topic*, int32_t, int) {
            assert(msgCur <= mockRetMessages.size());
            if (msgCur < totalMessageCount) {
                MockMessage *mockRetMessage = new MockMessage;
                EXPECT_CALL(*mockRetMessage, err())
                    .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
                EXPECT_CALL(*mockRetMessage, key())
                    .WillRepeatedly(Return(&key));
                EXPECT_CALL(*mockRetMessage, payload())
                    .WillRepeatedly(Return((void*)payload.c_str()));
                RdKafka::MessageTimestamp ts{
                    RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, msgCur++};
                EXPECT_CALL(*mockRetMessage, timestamp())
                    .WillRepeatedly(Return(ts));
                return mockRetMessage;
            } else {
                MockMessage *mockRetMessage = new MockMessage;
                EXPECT_CALL(*mockRetMessage, err())
                    .WillRepeatedly(Return(RdKafka::ERR__PARTITION_EOF));
                    return mockRetMessage;
            }
        };
    EXPECT_CALL(*mockConsumer, consume(_, _, _)).WillRepeatedly(mockConsumeFunc);
    int64_t timestamp;
    std::shared_ptr<RdKafka::Message> message;
    RdKafka::KafkaClientErrorCode ec(RdKafka::ERR_NO_ERROR);
    int64_t seekTimestamp = 3;
    size_t msgCount = 0;
    kafkaReaderWrapper.seekByTimestamp(seekTimestamp);
    while (true) {
        ec = kafkaReaderWrapper.read(timestamp, message, 3000);
        if (RdKafka::ERR_NO_ERROR != ec) {
            break;
        }
        msgCount++;
        EXPECT_TRUE(message->timestamp().timestamp >= seekTimestamp);
    }
    EXPECT_EQ(4, msgCount);
    // kafka eof
    ec = kafkaReaderWrapper.read(timestamp, message, 3000);
    EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);

    seekTimestamp = 0;
    msgCount = 0;
    kafkaReaderWrapper.seekByTimestamp(seekTimestamp);
    while (true) {
        ec = kafkaReaderWrapper.read(timestamp, message, 3000);
        if (RdKafka::ERR_NO_ERROR != ec) {
            break;
        }
        msgCount++;
        EXPECT_TRUE(message->timestamp().timestamp >= seekTimestamp);
    }
    EXPECT_EQ(7, msgCount);
    // kafka eof
    ec = kafkaReaderWrapper.read(timestamp, message, 3000);
    EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
}

TEST_F(KafkaReaderWrapperTest, testGetPartRangeVec) {
    MockKafkaReaderWrapper kafkaReaderWrapper;
    auto toRange = [](uint32_t from, uint32_t to) {
                       proto::Range range;
                       range.set_from(from);
                       range.set_to(to);
                       return range;
                   };
    {
        proto::Range range;
        range.set_from(0);
        range.set_to(65535);
        size_t kafkaPartCount = 3;
        auto partRangeVec = kafkaReaderWrapper.getPartRangeVec(range, kafkaPartCount);
        std::vector<std::pair<int32_t, proto::Range>> expectPartRangeVec = {
            {0, toRange(0, 21845)},
            {1, toRange(21846, 43690)},
            {2, toRange(43691, 65535)}
        };
        ASSERT_EQ(expectPartRangeVec.size(), partRangeVec.size());
        for (size_t i = 0; i < expectPartRangeVec.size(); ++i) {
            EXPECT_EQ(expectPartRangeVec[i].first, partRangeVec[i].first);
            EXPECT_EQ(expectPartRangeVec[i].second.from(), partRangeVec[i].second.from());
            EXPECT_EQ(expectPartRangeVec[i].second.to(), partRangeVec[i].second.to());
        }
    }
    {
        proto::Range range;
        range.set_from(11111);
        range.set_to(55555);
        size_t kafkaPartCount = 3;
        auto partRangeVec = kafkaReaderWrapper.getPartRangeVec(range, kafkaPartCount);
        std::vector<std::pair<int32_t, proto::Range>> expectPartRangeVec = {
            {0, toRange(11111, 21845)},
            {1, toRange(21846, 43690)},
            {2, toRange(43691, 55555)}
        };
        ASSERT_EQ(expectPartRangeVec.size(), partRangeVec.size());
        for (size_t i = 0; i < expectPartRangeVec.size(); ++i) {
            EXPECT_EQ(expectPartRangeVec[i].first, partRangeVec[i].first);
            EXPECT_EQ(expectPartRangeVec[i].second.from(), partRangeVec[i].second.from());
            EXPECT_EQ(expectPartRangeVec[i].second.to(), partRangeVec[i].second.to());
        }
    }
    {
        proto::Range range;
        range.set_from(11111);
        range.set_to(33333);
        size_t kafkaPartCount = 3;
        auto partRangeVec = kafkaReaderWrapper.getPartRangeVec(range, kafkaPartCount);
        std::vector<std::pair<int32_t, proto::Range>> expectPartRangeVec = {
            {0, toRange(11111, 21845)},
            {1, toRange(21846, 33333)}
        };
        ASSERT_EQ(expectPartRangeVec.size(), partRangeVec.size());
        for (size_t i = 0; i < expectPartRangeVec.size(); ++i) {
            EXPECT_EQ(expectPartRangeVec[i].first, partRangeVec[i].first);
            EXPECT_EQ(expectPartRangeVec[i].second.from(), partRangeVec[i].second.from());
            EXPECT_EQ(expectPartRangeVec[i].second.to(), partRangeVec[i].second.to());
        }
    }
    {
        proto::Range range;
        range.set_from(22222);
        range.set_to(33333);
        size_t kafkaPartCount = 3;
        auto partRangeVec = kafkaReaderWrapper.getPartRangeVec(range, kafkaPartCount);
        std::vector<std::pair<int32_t, proto::Range>> expectPartRangeVec = {
            {1, toRange(22222, 33333)}
        };
        ASSERT_EQ(expectPartRangeVec.size(), partRangeVec.size());
        for (size_t i = 0; i < expectPartRangeVec.size(); ++i) {
            EXPECT_EQ(expectPartRangeVec[i].first, partRangeVec[i].first);
            EXPECT_EQ(expectPartRangeVec[i].second.from(), partRangeVec[i].second.from());
            EXPECT_EQ(expectPartRangeVec[i].second.to(), partRangeVec[i].second.to());
        }
    }
    {
        proto::Range range;
        range.set_from(1234);
        range.set_to(1234);
        size_t kafkaPartCount = 3;
        auto partRangeVec = kafkaReaderWrapper.getPartRangeVec(range, kafkaPartCount);
        std::vector<std::pair<int32_t, proto::Range>> expectPartRangeVec = {
            {0, toRange(1234, 1234)}
        };
        ASSERT_EQ(expectPartRangeVec.size(), partRangeVec.size());
        for (size_t i = 0; i < expectPartRangeVec.size(); ++i) {
            EXPECT_EQ(expectPartRangeVec[i].first, partRangeVec[i].first);
            EXPECT_EQ(expectPartRangeVec[i].second.from(), partRangeVec[i].second.from());
            EXPECT_EQ(expectPartRangeVec[i].second.to(), partRangeVec[i].second.to());
        }
    }
}

TEST_F(KafkaReaderWrapperTest, testSimpleProcess_DisabledIntegration) {
    KafkaReaderParam param;
    param.topicName = "quickstart-events";
    param.range.set_from(0);
    param.range.set_to(65535);

    KeyValueMap kvMap;
    kvMap["bootstrap.servers"] = "localhost:9092";

    KafkaReaderWrapper kafkaReaderWrapper;
    ASSERT_TRUE(kafkaReaderWrapper.init(param, kvMap));

    int64_t timestamp;
    std::shared_ptr<RdKafka::Message> message;

    size_t num = _messages.size();
    RdKafka::KafkaClientErrorCode ec(RdKafka::ERR_NO_ERROR);
    for (size_t i = 0; i < num; ++i) {
        ec = kafkaReaderWrapper.read(timestamp, message, 3000);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, ec);
        printMessage(message);
    }
    // kafka eof
    ec = kafkaReaderWrapper.read(timestamp, message, 3000);
    EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
    // wait no more message timeout eof
    ec = kafkaReaderWrapper.read(timestamp, message, 3000);
    EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
    // wait no more message timeout eof
    ec = kafkaReaderWrapper.read(timestamp, message, 3000);
    EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
}

TEST_F(KafkaReaderWrapperTest, testReadUntilEof_DisabledIntegration) {
    KafkaReaderParam param;
    param.topicName = "quickstart-events";
    param.range.set_from(0);
    param.range.set_to(65535);

    KeyValueMap kvMap;
    kvMap["bootstrap.servers"] = "localhost:9092";

    KafkaReaderWrapper kafkaReaderWrapper;
    ASSERT_TRUE(kafkaReaderWrapper.init(param, kvMap));

    int64_t timestamp;
    std::shared_ptr<RdKafka::Message> message;
    RdKafka::KafkaClientErrorCode ec(RdKafka::ERR_NO_ERROR);
    size_t msgCount = 0;
    while (true) {
        ec = kafkaReaderWrapper.read(timestamp, message, 3000);
        if (RdKafka::ERR_NO_ERROR != ec) {
            if(RdKafka::ERR__PARTITION_EOF == ec) {
                std::cout << "eof" << std::endl;
            } else if(RdKafka::ERR__TIMED_OUT == ec) {
                std::cout << "timeout" << std::endl;
            } else {
                std::cout << RdKafka::err2str(ec) << std::endl;
            }
            EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
            break;
        }
        msgCount++;
        printMessage(message);
    }
    EXPECT_EQ(_messages.size(), msgCount);
}

TEST_F(KafkaReaderWrapperTest, testReadWithTimestampLimit_DisabledIntegration) {
    KafkaReaderParam param;
    param.topicName = "quickstart-events";
    param.range.set_from(0);
    param.range.set_to(65535);

    KeyValueMap kvMap;
    kvMap["bootstrap.servers"] = "localhost:9092";

    KafkaReaderWrapper kafkaReaderWrapper;
    ASSERT_TRUE(kafkaReaderWrapper.init(param, kvMap));

    int64_t timestamp;
    std::shared_ptr<RdKafka::Message> message;

    RdKafka::KafkaClientErrorCode ec(RdKafka::ERR_NO_ERROR);
    size_t limitMsgIdx = _messages.size() / 3;
    int64_t limitTimestamp = _messages[limitMsgIdx]->timestamp().timestamp;
    int64_t acceptTimestamp;
    kafkaReaderWrapper.setTimestampLimit(limitTimestamp, acceptTimestamp);
    std::cout << "acceptTimestamp:" << acceptTimestamp << std::endl;
    size_t msgCount = 0;
    while (true) {
        ec = kafkaReaderWrapper.read(timestamp, message, 3000);
        if (RdKafka::ERR_NO_ERROR != ec) {
            if(RdKafka::ERR__PARTITION_EOF == ec) {
                std::cout << "eof" << std::endl;
            } else if(RdKafka::ERR__TIMED_OUT == ec) {
                std::cout << "timeout" << std::endl;
            } else {
                std::cout << RdKafka::err2str(ec) << std::endl;
            }
            EXPECT_EQ(RdKafka::ERR_EX_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec.code());
            break;
        }
        msgCount++;
        printMessage(message);
    }
    EXPECT_EQ(limitMsgIdx + 1, msgCount);

    limitMsgIdx = _messages.size() / 2;
    limitTimestamp = _messages[limitMsgIdx]->timestamp().timestamp;
    kafkaReaderWrapper.setTimestampLimit(limitTimestamp, acceptTimestamp);
    std::cout << "acceptTimestamp:" << acceptTimestamp << std::endl;
    while (true) {
        ec = kafkaReaderWrapper.read(timestamp, message, 3000);
        if (RdKafka::ERR_NO_ERROR != ec) {
            if(RdKafka::ERR__PARTITION_EOF == ec) {
                std::cout << "eof" << std::endl;
            } else if(RdKafka::ERR__TIMED_OUT == ec) {
                std::cout << "timeout" << std::endl;
            } else {
                std::cout << RdKafka::err2str(ec) << std::endl;
            }
            EXPECT_EQ(RdKafka::ERR_EX_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec.code());
            break;
        }
        msgCount++;
        printMessage(message);
    }
    EXPECT_EQ(limitMsgIdx + 1, msgCount);
}

TEST_F(KafkaReaderWrapperTest, testSeekByTimeStamp_DisabledIntegration) {
    KafkaReaderParam param;
    param.topicName = "quickstart-events";
    param.range.set_from(0);
    param.range.set_to(65535);

    KeyValueMap kvMap;
    kvMap["bootstrap.servers"] = "localhost:9092";

    KafkaReaderWrapper kafkaReaderWrapper;
    ASSERT_TRUE(kafkaReaderWrapper.init(param, kvMap));

    int64_t timestamp;
    std::shared_ptr<RdKafka::Message> message;

    {
        size_t seekMessageIndex = _messages.size() / 2;
        int64_t seekTimestamp = _messages[seekMessageIndex]->timestamp().timestamp;
        RdKafka::KafkaClientErrorCode ec(RdKafka::ERR_NO_ERROR);
        ec = kafkaReaderWrapper.seekByTimestamp(seekTimestamp);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, ec);
        size_t msgCount = 0;
        while (true) {
            ec = kafkaReaderWrapper.read(timestamp, message, 3000);
            if (RdKafka::ERR_NO_ERROR != ec) {
                if(RdKafka::ERR__PARTITION_EOF == ec) {
                    std::cout << "eof" << std::endl;
                } else if(RdKafka::ERR__TIMED_OUT == ec) {
                    std::cout << "timeout" << std::endl;
                } else {
                    std::cout << RdKafka::err2str(ec) << std::endl;
                }
                EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
                break;
            }
            msgCount++;
            printMessage(message);
            EXPECT_TRUE(seekTimestamp <= message->timestamp().timestamp);
        }
        EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
        EXPECT_EQ(_messages.size() - seekMessageIndex, msgCount);
    }
    {
        size_t seekMessageIndex = _messages.size() / 3;
        int64_t seekTimestamp = _messages[seekMessageIndex]->timestamp().timestamp;
        RdKafka::KafkaClientErrorCode ec(RdKafka::ERR_NO_ERROR);
        ec = kafkaReaderWrapper.seekByTimestamp(seekTimestamp);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, ec);
        size_t msgCount = 0;
        while (true) {
            ec = kafkaReaderWrapper.read(timestamp, message, 3000);
            if (RdKafka::ERR_NO_ERROR != ec) {
                if(RdKafka::ERR__PARTITION_EOF == ec) {
                    std::cout << "eof" << std::endl;
                } else if(RdKafka::ERR__TIMED_OUT == ec) {
                    std::cout << "timeout" << std::endl;
                } else {
                    std::cout << RdKafka::err2str(ec) << std::endl;
                }
                EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
                break;
            }
            msgCount++;
            printMessage(message);
            EXPECT_TRUE(seekTimestamp <= message->timestamp().timestamp);
        }
        EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
        EXPECT_EQ(_messages.size() - seekMessageIndex, msgCount);
    }
}

}
}
