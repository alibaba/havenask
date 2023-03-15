#include <unittest/unittest.h>
#include <autil/StringUtil.h>
#include <librdkafka/rdkafkacpp.h>
#include "raw_doc_readers/KafkaSinglePartitionConsumerWrapper.h"
#include "MockKafkaConsumer.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service;

namespace pluginplatform {
namespace reader_plugins {

class KafkaSinglePartitionConsumerWrapperTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void KafkaSinglePartitionConsumerWrapperTest::setUp() {
}

void KafkaSinglePartitionConsumerWrapperTest::tearDown() {
}

TEST_F(KafkaSinglePartitionConsumerWrapperTest, testStart) {
    MockKafkaConsumer *mockConsumer = new MockKafkaConsumer;
    std::shared_ptr<RdKafka::Consumer> consumer(mockConsumer);
    std::shared_ptr<RdKafka::Topic> topic;
    KafkaSinglePartitionConsumerConfig config;
    build_service::proto::Range range;
    int32_t partition = 2;
    KafkaSinglePartitionConsumerWrapper partConsumer(config,
            consumer, topic, partition, range);
    {
        EXPECT_CALL(*mockConsumer, start(_, _, _)).Times(1);
        RdKafka::ErrorCode ec = partConsumer.start(topic, RdKafka::Topic::OFFSET_BEGINNING);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, ec);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, partConsumer._lastErrorCode);
    }
    {
        EXPECT_CALL(*mockConsumer, start(_, _, _))
            .Times(1).WillOnce(Return(RdKafka::ERR_UNKNOWN));
        RdKafka::ErrorCode ec = partConsumer.start(topic, RdKafka::Topic::OFFSET_BEGINNING);
        EXPECT_EQ(RdKafka::ERR_UNKNOWN, ec);
        EXPECT_EQ(RdKafka::ERR_UNKNOWN, partConsumer._lastErrorCode);
    }
    {
        EXPECT_CALL(*mockConsumer, start(_, _, _)).Times(1);
        RdKafka::ErrorCode ec = partConsumer.start(topic, RdKafka::Topic::OFFSET_BEGINNING);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, ec);
        EXPECT_EQ(RdKafka::ERR_UNKNOWN, partConsumer._lastErrorCode);
    }
}

TEST_F(KafkaSinglePartitionConsumerWrapperTest, testSeek) {
    MockKafkaConsumer *mockConsumer = new MockKafkaConsumer;
    std::shared_ptr<RdKafka::Consumer> consumer(mockConsumer);
    std::shared_ptr<RdKafka::Topic> topic;
    KafkaSinglePartitionConsumerConfig config;
    build_service::proto::Range range;
    int32_t partition = 2;
    KafkaSinglePartitionConsumerWrapper partConsumer(config,
            consumer, topic, partition, range);
    {
        EXPECT_CALL(*mockConsumer, seek(_, _, _, _))
            .Times(1).WillOnce(Return(RdKafka::ERR_NO_ERROR));
        int64_t timestamp = 1234;
        RdKafka::ErrorCode ec = partConsumer.seek(0, 1234, 3000);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, ec);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, partConsumer._lastErrorCode);
        EXPECT_EQ(-1, partConsumer._lastMsgTimestamp);
        EXPECT_EQ(timestamp, partConsumer._seekTimestamp);
    }
    {
        partConsumer._lastMsgTimestamp = 5678;
        partConsumer._seekTimestamp = 4321;
        EXPECT_CALL(*mockConsumer, seek(_, _, _, _))
            .Times(1).WillOnce(Return(RdKafka::ERR_UNKNOWN));
        RdKafka::ErrorCode ec = partConsumer.seek(0, 1234, 3000);
        EXPECT_EQ(RdKafka::ERR_UNKNOWN, ec);
        EXPECT_EQ(RdKafka::ERR_UNKNOWN, partConsumer._lastErrorCode);
        EXPECT_EQ(5678, partConsumer._lastMsgTimestamp);
        EXPECT_EQ(4321, partConsumer._seekTimestamp);
    }
}

TEST_F(KafkaSinglePartitionConsumerWrapperTest, testConsumeOne) {
    MockKafkaConsumer *mockConsumer = new MockKafkaConsumer;
    MockMessage *mockRetMessage = new MockMessage;
    std::shared_ptr<RdKafka::Consumer> consumer(mockConsumer);
    std::shared_ptr<RdKafka::Topic> topic;
    KafkaSinglePartitionConsumerConfig config;
    build_service::proto::Range range;
    int32_t partition = 2;
    KafkaSinglePartitionConsumerWrapper partConsumer(config,
            consumer, topic, partition, range);

    {
        std::shared_ptr<RdKafka::Message> message;
        EXPECT_CALL(*mockConsumer, consume(_, _, _))
            .Times(1).WillOnce(Return(mockRetMessage));
        EXPECT_CALL(*mockConsumer, poll(_)).Times(1);
        EXPECT_CALL(*mockRetMessage, err())
            .Times(1).WillOnce(Return(RdKafka::ERR__PARTITION_EOF));
        RdKafka::ErrorCode ec = partConsumer.consumeOne(message);
        EXPECT_EQ(RdKafka::ERR__PARTITION_EOF, ec);
        EXPECT_FALSE(message.get());
    }
    {
        std::shared_ptr<RdKafka::Message> message;
        EXPECT_CALL(*mockConsumer, consume(_, _, _))
            .Times(1).WillOnce(Return(nullptr));
        RdKafka::ErrorCode ec = partConsumer.consumeOne(message);
        EXPECT_EQ(RdKafka::ERR_UNKNOWN, ec);
    }
    {
        std::shared_ptr<RdKafka::Message> message;
        std::string key("test_key");
        MockMessage *mockRetMessage = new MockMessage;
        EXPECT_CALL(*mockConsumer, consume(_, _, _))
            .Times(1).WillOnce(Return(mockRetMessage));
        EXPECT_CALL(*mockConsumer, poll(_)).Times(1);
        EXPECT_CALL(*mockRetMessage, err())
            .Times(1).WillOnce(Return(RdKafka::ERR_NO_ERROR));
        EXPECT_CALL(*mockRetMessage, key())
            .Times(2).WillRepeatedly(Return(&key));
        RdKafka::ErrorCode ec = partConsumer.consumeOne(message);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, ec);
        EXPECT_FALSE(!message.get());
    }
}

TEST_F(KafkaSinglePartitionConsumerWrapperTest, testConsumeBatch) {
    MockKafkaConsumer *mockConsumer = new MockKafkaConsumer;
    std::shared_ptr<RdKafka::Consumer> consumer(mockConsumer);
    std::shared_ptr<RdKafka::Topic> topic;
    KafkaSinglePartitionConsumerConfig config;
    build_service::proto::Range range;
    int32_t partition = 2;
    KafkaSinglePartitionConsumerWrapper partConsumer(config,
            consumer, topic, partition, range);

    {
        std::string key("test_key");
        partConsumer._config.batchConsumeCount = 2;
        vector<MockMessage *> mockRetMessages;
        for (size_t i = 0; i < partConsumer._config.batchConsumeCount; ++i) {
            MockMessage *mockRetMessage = new MockMessage;
            mockRetMessages.push_back(mockRetMessage);
            EXPECT_CALL(*mockRetMessage, err())
                .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
            EXPECT_CALL(*mockRetMessage, key())
                .WillRepeatedly(Return(&key));
        }
        auto mockConsumeFunc = [&](RdKafka::Topic*, int32_t, int){
                               static size_t cnt = 0;
                               assert(cnt <= mockRetMessages.size());
                               return mockRetMessages[cnt++];
                           };
        std::vector<std::shared_ptr<RdKafka::Message>> messages;
        EXPECT_CALL(*mockConsumer, consume(_, _, _))
            .WillRepeatedly(mockConsumeFunc);
        EXPECT_CALL(*mockConsumer, poll(_))
            .WillRepeatedly(Return(0));
        RdKafka::ErrorCode ec = partConsumer.consumeBatch(messages);
        EXPECT_EQ(RdKafka::ERR_NO_ERROR, ec);
        EXPECT_EQ(partConsumer._config.batchConsumeCount, messages.size());
    }
}

TEST_F(KafkaSinglePartitionConsumerWrapperTest, testRangeFilter) {
    MockKafkaConsumer *mockConsumer = new MockKafkaConsumer;
    std::shared_ptr<RdKafka::Consumer> consumer(mockConsumer);
    std::shared_ptr<RdKafka::Topic> topic;
    KafkaSinglePartitionConsumerConfig config;
    build_service::proto::Range range;
    int32_t partition = 2;
    KafkaSinglePartitionConsumerWrapper partConsumer(config,
            consumer, topic, partition, range);
    {
        MockMessage *mockMessage = new MockMessage;
        std::shared_ptr<RdKafka::Message> message(mockMessage);
        std::string key("test_key");
        EXPECT_CALL(*mockMessage, key())
            .Times(2).WillRepeatedly(Return(&key));
        EXPECT_TRUE(partConsumer.rangeFilter(message));
    }
    {
        std::shared_ptr<RdKafka::Message> message;
        EXPECT_FALSE(partConsumer.rangeFilter(message));
    }
    {
        MockMessage *mockMessage = new MockMessage;
        std::shared_ptr<RdKafka::Message> message(mockMessage);
        EXPECT_CALL(*mockMessage, key())
            .Times(1).WillRepeatedly(Return(nullptr));
        EXPECT_FALSE(partConsumer.rangeFilter(message));
    }
    {
        MockMessage *mockMessage = new MockMessage;
        std::shared_ptr<RdKafka::Message> message(mockMessage);
        std::string key("test_key");
        uint32_t hashId = partConsumer.hashFunc(key);
        partConsumer._range.set_from(hashId+1);
        EXPECT_CALL(*mockMessage, key())
            .Times(2).WillRepeatedly(Return(&key));
        EXPECT_FALSE(partConsumer.rangeFilter(message));
    }
}

TEST_F(KafkaSinglePartitionConsumerWrapperTest, testExceedTimestampLimit) {
    MockKafkaConsumer *mockConsumer = new MockKafkaConsumer;
    std::shared_ptr<RdKafka::Consumer> consumer(mockConsumer);
    std::shared_ptr<RdKafka::Topic> topic;
    KafkaSinglePartitionConsumerConfig config;
    build_service::proto::Range range;
    int32_t partition = 2;
    KafkaSinglePartitionConsumerWrapper partConsumer(config,
            consumer, topic, partition, range);
    {
        partConsumer._timestampLimit = numeric_limits<int64_t>::max();
        EXPECT_FALSE(partConsumer.exceedTimestampLimit());
    }
    {
        partConsumer._timestampLimit = 1234;
        EXPECT_FALSE(partConsumer.exceedTimestampLimit());
    }
    {
        MockMessage *mockMessage = new MockMessage;
        std::shared_ptr<RdKafka::Message> message(mockMessage);
        RdKafka::MessageTimestamp ts{
            RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 4567};
        EXPECT_CALL(*mockMessage, timestamp())
            .Times(1).WillRepeatedly(Return(ts));
        partConsumer._buffer.push_back(message);
        partConsumer._timestampLimit = 1234;
        EXPECT_TRUE(partConsumer.exceedTimestampLimit());
    }
}

TEST_F(KafkaSinglePartitionConsumerWrapperTest, testTryRead) {
    MockKafkaConsumer *mockConsumer = new MockKafkaConsumer;
    std::shared_ptr<RdKafka::Consumer> consumer(mockConsumer);
    std::shared_ptr<RdKafka::Topic> topic;
    KafkaSinglePartitionConsumerConfig config;
    build_service::proto::Range range;
    int32_t partition = 2;
    KafkaSinglePartitionConsumerWrapper partConsumer(config,
            consumer, topic, partition, range);
    {
        std::string key("test_key");
        partConsumer._config.batchReadCount = 3;
        vector<MockMessage *> mockRetMessages;
        for (size_t i = 0; i < partConsumer._config.batchReadCount; ++i) {
            MockMessage *mockRetMessage = new MockMessage;
            mockRetMessages.push_back(mockRetMessage);
            partConsumer._buffer.push_back(
                    std::shared_ptr<RdKafka::Message>(mockRetMessage));
            RdKafka::MessageTimestamp ts{
                RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, i};
            EXPECT_CALL(*mockRetMessage, timestamp())
                .Times(2).WillRepeatedly(Return(ts));
        }
        std::vector<std::shared_ptr<RdKafka::Message>> messages;
        EXPECT_TRUE(partConsumer.tryRead(messages));
        EXPECT_EQ(partConsumer._config.batchReadCount, messages.size());
    }
    {
        std::string key("test_key");
        partConsumer._config.batchReadCount = 3;
        vector<MockMessage *> mockRetMessages;
        for (size_t i = 0; i < partConsumer._config.batchReadCount; ++i) {
            MockMessage *mockRetMessage = new MockMessage;
            mockRetMessages.push_back(mockRetMessage);
            partConsumer._buffer.push_back(
                    std::shared_ptr<RdKafka::Message>(mockRetMessage));
            RdKafka::MessageTimestamp ts{
                RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, i};
            EXPECT_CALL(*mockRetMessage, timestamp())
                .WillRepeatedly(Return(ts));
        }
        partConsumer._timestampLimit = 1;
        std::vector<std::shared_ptr<RdKafka::Message>> messages;
        EXPECT_TRUE(partConsumer.tryRead(messages));
        EXPECT_EQ(2, messages.size());
    }
}

TEST_F(KafkaSinglePartitionConsumerWrapperTest, testTryFillBuffer) {
    MockKafkaConsumer *mockConsumer = new MockKafkaConsumer;
    std::shared_ptr<RdKafka::Consumer> consumer(mockConsumer);
    std::shared_ptr<RdKafka::Topic> topic;
    KafkaSinglePartitionConsumerConfig config;
    build_service::proto::Range range;
    int32_t partition = 2;
    KafkaSinglePartitionConsumerWrapper partConsumer(config,
            consumer, topic, partition, range);
    {
        std::string key("test_key");
        partConsumer._config.batchConsumeCount = 2;
        vector<MockMessage *> mockRetMessages;
        for (size_t i = 0; i < partConsumer._config.batchConsumeCount; ++i) {
            MockMessage *mockRetMessage = new MockMessage;
            mockRetMessages.push_back(mockRetMessage);
            EXPECT_CALL(*mockRetMessage, err())
                .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
            EXPECT_CALL(*mockRetMessage, key())
                .WillRepeatedly(Return(&key));
        }
        auto mockConsumeFunc = [&](RdKafka::Topic*, int32_t, int){
                               static size_t cnt = 0;
                               assert(cnt <= mockRetMessages.size());
                               return mockRetMessages[cnt++];
                           };
        EXPECT_CALL(*mockConsumer, consume(_, _, _))
            .WillRepeatedly(mockConsumeFunc);
        EXPECT_CALL(*mockConsumer, poll(_))
            .WillRepeatedly(Return(0));
        EXPECT_TRUE(partConsumer.tryFillBuffer());
        EXPECT_EQ(partConsumer._config.batchConsumeCount, partConsumer._buffer.size());
    }
    {
        partConsumer._buffer.clear();
        MockMessage *mockMessage = new MockMessage;
        std::shared_ptr<RdKafka::Message> message(mockMessage);
        RdKafka::MessageTimestamp ts{
            RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 4567};
        EXPECT_CALL(*mockMessage, timestamp())
            .Times(1).WillRepeatedly(Return(ts));
        partConsumer._buffer.push_back(message);
        partConsumer._timestampLimit = 1234;
        EXPECT_FALSE(partConsumer.tryFillBuffer());
        EXPECT_EQ(1, partConsumer._buffer.size());
    }
}

}
}
