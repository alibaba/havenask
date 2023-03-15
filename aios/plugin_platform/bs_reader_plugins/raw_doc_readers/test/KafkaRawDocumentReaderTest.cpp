#include <unittest/unittest.h>
#include <autil/StringUtil.h>
#include "build_service/reader/RawDocumentReader.h"
#include "raw_doc_readers/KafkaRawDocumentReader.h"
#include "MockKafkaConsumer.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service;
using namespace build_service::reader;

namespace pluginplatform {
namespace reader_plugins {

static const std::string KAFKA_TOPIC_NAME = "topic_name";
static const std::string KAFKA_START_TIMESTAMP = "kafka_start_timestamp";
static const std::string KAFKA_STOP_TIMESTAMP = "kafka_stop_timestamp";
static const std::string KAFKA_SUSPEND_TIMESTAMP = "kafka_suspend_timestamp";

class MockKafkaReaderWrapperRawDoc : public KafkaReaderWrapper {
public:
    MOCK_METHOD2(createConsumer, RdKafka::Consumer* (
                    std::shared_ptr<RdKafka::Conf> &conf, std::string &errStr));
    MOCK_METHOD4(createTopic, RdKafka::Topic* (
            std::shared_ptr<RdKafka::Consumer> &consumer, std::string &topicName,
            std::shared_ptr<RdKafka::Conf> &conf, std::string &errStr));
    MOCK_METHOD3(read, RdKafka::KafkaClientErrorCode (int64_t &timestamp,
                    std::shared_ptr<RdKafka::Message> &message,
                    int64_t timeout));
};

class KafkaRawDocumentReaderTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void KafkaRawDocumentReaderTest::setUp() {
}

void KafkaRawDocumentReaderTest::tearDown() {
}

TEST_F(KafkaRawDocumentReaderTest, testSetLimitTime) {
    KafkaRawDocumentReader reader;
    KafkaReaderWrapperPtr kafkaReader(new KafkaReaderWrapper);
    {
        KeyValueMap kvMap;
        EXPECT_TRUE(reader.setLimitTime(kafkaReader, kvMap, KAFKA_SUSPEND_TIMESTAMP));
    }
    {
        KeyValueMap kvMap;
        EXPECT_TRUE(reader.setLimitTime(kafkaReader, kvMap, KAFKA_STOP_TIMESTAMP));
    }
    {
        KeyValueMap kvMap;
        kvMap[KAFKA_STOP_TIMESTAMP] = "abc";
        EXPECT_FALSE(reader.setLimitTime(kafkaReader, kvMap, KAFKA_STOP_TIMESTAMP));
        EXPECT_EQ(RawDocumentReader::ETA_STOP, reader._exceedTsAction);
        EXPECT_EQ(false, kafkaReader->_timeLimitMode);
    }
    {
        KeyValueMap kvMap;
        kvMap[KAFKA_SUSPEND_TIMESTAMP] = "1234";
        EXPECT_TRUE(reader.setLimitTime(kafkaReader, kvMap, KAFKA_SUSPEND_TIMESTAMP));
        EXPECT_EQ(RawDocumentReader::ETA_SUSPEND, reader._exceedTsAction);
        EXPECT_EQ(true, kafkaReader->_timeLimitMode);
    }
    {
        KeyValueMap kvMap;
        kvMap[KAFKA_STOP_TIMESTAMP] = "1234";
        EXPECT_TRUE(reader.setLimitTime(kafkaReader, kvMap, KAFKA_STOP_TIMESTAMP));
        EXPECT_EQ(RawDocumentReader::ETA_STOP, reader._exceedTsAction);
        EXPECT_EQ(true, kafkaReader->_timeLimitMode);
    }
}

TEST_F(KafkaRawDocumentReaderTest, testReadDocStr) {
    KafkaRawDocumentReader reader;
    MockKafkaReaderWrapperRawDoc* mockKafkaReader = new MockKafkaReaderWrapperRawDoc;
    std::string payload = "test_payload";
    std::string key = "test_key";
    reader._kafkaReader.reset(mockKafkaReader);
    RdKafka::KafkaClientErrorCode readErrorCode = RdKafka::ERR_NO_ERROR;
    auto readFunc =
        [&](int64_t &timestamp, std::shared_ptr<RdKafka::Message> &message,
            int64_t timeout) -> RdKafka::KafkaClientErrorCode
        {
            MockMessage *mockRetMessage = new MockMessage;
            std::shared_ptr<RdKafka::Message> retMessage(mockRetMessage);
            EXPECT_CALL(*mockRetMessage, err())
                .WillRepeatedly(Return(RdKafka::ERR_NO_ERROR));
            EXPECT_CALL(*mockRetMessage, key())
                .WillRepeatedly(Return(&key));
            EXPECT_CALL(*mockRetMessage, payload())
                .WillRepeatedly(Return((void*)payload.c_str()));
            RdKafka::MessageTimestamp ts{
                RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 0};
            EXPECT_CALL(*mockRetMessage, timestamp())
                .WillRepeatedly(Return(ts));
            retMessage.swap(message);
            return readErrorCode;
        };
    {
        readErrorCode = RdKafka::ERR_NO_ERROR;
        EXPECT_CALL(*mockKafkaReader, read(_, _, _))
            .WillRepeatedly(readFunc);
        std::string docStr;
        int64_t offset;
        int64_t msgTimestamp;
        RawDocumentReader::ErrorCode ec = reader.readDocStr(docStr, offset, msgTimestamp);
        EXPECT_EQ(RawDocumentReader::ERROR_NONE, ec);
    }
    {
        readErrorCode = RdKafka::ERR_EX_CLIENT_EXCEED_TIME_STAMP_LIMIT;
        reader._exceedTsAction = RawDocumentReader::ETA_SUSPEND;
        EXPECT_CALL(*mockKafkaReader, read(_, _, _))
            .WillRepeatedly(readFunc);
        std::string docStr;
        int64_t offset;
        int64_t msgTimestamp;
        RawDocumentReader::ErrorCode ec = reader.readDocStr(docStr, offset, msgTimestamp);
        EXPECT_EQ(RawDocumentReader::ERROR_WAIT, ec);
    }
    {
        readErrorCode = RdKafka::ERR__PARTITION_EOF;
        reader._exceedTsAction = RawDocumentReader::ETA_SUSPEND;
        EXPECT_CALL(*mockKafkaReader, read(_, _, _))
            .WillRepeatedly(readFunc);
        std::string docStr;
        int64_t offset;
        int64_t msgTimestamp;
        RawDocumentReader::ErrorCode ec = reader.readDocStr(docStr, offset, msgTimestamp);
        EXPECT_EQ(RawDocumentReader::ERROR_WAIT, ec);
    }
    {
        readErrorCode = RdKafka::ERR_UNKNOWN;
        reader._exceedTsAction = RawDocumentReader::ETA_SUSPEND;
        EXPECT_CALL(*mockKafkaReader, read(_, _, _))
            .WillRepeatedly(readFunc);
        std::string docStr;
        int64_t offset;
        int64_t msgTimestamp;
        RawDocumentReader::ErrorCode ec = reader.readDocStr(docStr, offset, msgTimestamp);
        EXPECT_EQ(RawDocumentReader::ERROR_WAIT, ec);
    }
}

}
}
