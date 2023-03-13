#ifndef ISEARCH_BS_KAFKASINGLEPARTITIONCONSUMERWRAPPER_H
#define ISEARCH_BS_KAFKASINGLEPARTITIONCONSUMERWRAPPER_H

#include "build_service/util/Log.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include <librdkafka/rdkafkacpp.h>

namespace pluginplatform {
namespace reader_plugins {

struct KafkaSinglePartitionConsumerConfig {
    uint64_t batchReadCount;
    uint64_t batchConsumeCount;
    uint64_t readBufferSize;
};

class KafkaSinglePartitionConsumerWrapper {
public:
    KafkaSinglePartitionConsumerWrapper(const KafkaSinglePartitionConsumerConfig &config,
            std::shared_ptr<RdKafka::Consumer> &consumer,
            std::shared_ptr<RdKafka::Topic> &topic,
            int32_t partition,
            build_service::proto::Range range);
    ~KafkaSinglePartitionConsumerWrapper();

    int64_t getFirstMsgTimestamp() const;
    int64_t getNextMsgTimestamp() const;
    int64_t getLastMsgTimestamp() const;
    bool tryRead(std::vector<std::shared_ptr<RdKafka::Message>> &messages);
    bool tryFillBuffer();
    RdKafka::ErrorCode consumeOne(std::shared_ptr<RdKafka::Message> &message);
    RdKafka::ErrorCode consumeBatch(std::vector<std::shared_ptr<RdKafka::Message>> &messages);
    bool rangeFilter(std::shared_ptr<RdKafka::Message> &msg);
    RdKafka::ErrorCode seek(int64_t offset, int64_t timestamp, int timeout);
    RdKafka::ErrorCode start(std::shared_ptr<RdKafka::Topic> &topic, int64_t offset);
    RdKafka::ErrorCode stop();
    uint32_t hashFunc(const std::string &key);
    int32_t partition() const { return _partition; }
    build_service::proto::Range range() const { return _range; }
    void clear();
    RdKafka::ErrorCode reportFatalError();
    void checkErrorCode(RdKafka::ErrorCode ec);
    bool exceedTimestampLimit();
    void setTimestampLimit(int64_t timestampLimit);
private:
    KafkaSinglePartitionConsumerWrapper(const KafkaSinglePartitionConsumerWrapper &);
    KafkaSinglePartitionConsumerWrapper& operator=(const KafkaSinglePartitionConsumerWrapper &);
private:
    KafkaSinglePartitionConsumerConfig _config;
    std::shared_ptr<RdKafka::Consumer> _consumer;
    std::shared_ptr<RdKafka::Topic> _topic;

    int32_t _partition;
    build_service::proto::Range _range;
    std::vector<std::shared_ptr<RdKafka::Message>> _buffer;
    uint64_t _bufferCursor;
    int64_t _lastMsgTimestamp;
    int64_t _seekTimestamp;

    RdKafka::ErrorCode _lastErrorCode;
    int64_t _timestampLimit;
private:
    BS_LOG_DECLARE();
};

typedef std::shared_ptr<KafkaSinglePartitionConsumerWrapper> KafkaSinglePartitionConsumerWrapperPtr;

}
}

#endif //ISEARCH_BS_KAFKAREADERWRAPPER_H
