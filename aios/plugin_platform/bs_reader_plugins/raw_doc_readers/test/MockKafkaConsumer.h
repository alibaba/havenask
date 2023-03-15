#include <unittest/unittest.h>
#include <librdkafka/rdkafkacpp.h>

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service;

namespace pluginplatform {
namespace reader_plugins {

class MockKafkaConsumer : public RdKafka::Consumer {
public:
    MOCK_METHOD1(poll, int(int));
    MOCK_METHOD3(consume, RdKafka::Message*(RdKafka::Topic*, int32_t, int));
    MOCK_METHOD3(start, RdKafka::ErrorCode(RdKafka::Topic*, int32_t, int64_t));
    MOCK_METHOD2(stop, RdKafka::ErrorCode(RdKafka::Topic*, int32_t));
    MOCK_METHOD4(seek, RdKafka::ErrorCode(RdKafka::Topic*, int32_t, int64_t, int));
    MOCK_METHOD2(offsetsForTimes,
                 RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *>, int));
    MOCK_METHOD4(metadata,
                 RdKafka::ErrorCode(bool, const RdKafka::Topic*, RdKafka::Metadata**, int));

    // not used pure method
    MOCK_CONST_METHOD0(name, std::string());
    MOCK_CONST_METHOD0(memberid, std::string());
    MOCK_METHOD0(outq_len, int());
    MOCK_METHOD1(pause, RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition*>&));
    MOCK_METHOD1(resume, RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition*>&));
    MOCK_METHOD5(query_watermark_offsets,
                 RdKafka::ErrorCode(const std::string&, int32_t, int64_t*, int64_t*, int));
    MOCK_METHOD4(get_watermark_offsets,
                 RdKafka::ErrorCode(const std::string&, int32_t, int64_t*, int64_t*));
    MOCK_METHOD2(offsetsForTimes,
                 RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition*>&, int));
    MOCK_METHOD1(get_partition_queue, RdKafka::Queue*(const RdKafka::TopicPartition*));
    MOCK_METHOD1(set_log_queue, RdKafka::ErrorCode(RdKafka::Queue*));
    MOCK_METHOD0(yield, void());
    MOCK_METHOD1(clusterid, std::string(int));
    MOCK_METHOD0(c_ptr, struct rd_kafka_s*());
    MOCK_METHOD1(controllerid, int32_t(int));
    MOCK_CONST_METHOD1(fatal_error, RdKafka::ErrorCode(std::string&));
    MOCK_METHOD5(oauthbearer_set_token,
                 RdKafka::ErrorCode(const std::string&, int64_t, const string&,
                         const std::list<std::basic_string<char> >&, std::string&));
    MOCK_METHOD1(oauthbearer_set_token_failure, RdKafka::ErrorCode(const std::string&));
    MOCK_METHOD0(sasl_background_callbacks_enable, RdKafka::Error*());
    MOCK_METHOD0(get_sasl_queue, RdKafka::Queue*());
    MOCK_METHOD0(get_background_queue, RdKafka::Queue*());
    MOCK_METHOD1(mem_malloc, void*(size_t));
    MOCK_METHOD1(mem_free, void(void*));
    MOCK_METHOD2(sasl_set_credentials, RdKafka::Error*(const std::string&, const string&));
    MOCK_METHOD4(start,
                 RdKafka::ErrorCode(RdKafka::Topic*, int32_t, int64_t, RdKafka::Queue*));
    MOCK_METHOD2(consume, RdKafka::Message*(RdKafka::Queue*, int));
    MOCK_METHOD5(consume_callback,
                 int(RdKafka::Topic*, int32_t, int, RdKafka::ConsumeCb*, void*));
    MOCK_METHOD4(consume_callback, int(RdKafka::Queue*, int, RdKafka::ConsumeCb*, void*));
};

class MockMessage : public RdKafka::Message {
public:
    MOCK_CONST_METHOD0(errstr, std::string());
    MOCK_CONST_METHOD0(err, RdKafka::ErrorCode());
    MOCK_CONST_METHOD0(topic, RdKafka::Topic*());
    MOCK_CONST_METHOD0(topic_name, std::string());
    MOCK_CONST_METHOD0(partition, int32_t());
    MOCK_CONST_METHOD0(payload, void*());
    MOCK_CONST_METHOD0(len, size_t());
    MOCK_CONST_METHOD0(key_len, size_t());
    MOCK_CONST_METHOD0(offset, int64_t());
    MOCK_CONST_METHOD0(timestamp, RdKafka::MessageTimestamp());
    MOCK_CONST_METHOD0(msg_opaque, void*());
    MOCK_CONST_METHOD0(latency, int64_t());
    MOCK_METHOD0(c_ptr, rd_kafka_message_s*());
    MOCK_CONST_METHOD0(status, RdKafka::Message::Status());
    MOCK_METHOD0(headers, RdKafka::Headers*());
    MOCK_METHOD1(headers, RdKafka::Headers*(RdKafka::ErrorCode*));
    MOCK_CONST_METHOD0(broker_id, int32_t());
    MOCK_CONST_METHOD0(key, const string*());
    MOCK_CONST_METHOD0(key_pointer, const void*());
};

class MockTopicMetadata : public RdKafka::TopicMetadata {
public:
    MOCK_CONST_METHOD0(topic, std::string());
    MOCK_CONST_METHOD0(partitions, const RdKafka::TopicMetadata::PartitionMetadataVector*());
    MOCK_CONST_METHOD0(err, RdKafka::ErrorCode());
};

class MockMetadata : public RdKafka::Metadata {
public:
    MOCK_CONST_METHOD0(brokers, const RdKafka::Metadata::BrokerMetadataVector*());
    MOCK_CONST_METHOD0(topics, const RdKafka::Metadata::TopicMetadataVector*());
    MOCK_CONST_METHOD0(orig_broker_id, int32_t());
    MOCK_CONST_METHOD0(orig_broker_name, std::string());
};

class MockTopic : public RdKafka::Topic {
public:
    MOCK_CONST_METHOD0(name, std::string());
    MOCK_CONST_METHOD1(partition_available, bool(int32_t));
    MOCK_METHOD2(offset_store, RdKafka::ErrorCode(int32_t, int64_t));
    MOCK_METHOD0(c_ptr, rd_kafka_topic_s*());
};

}
}
