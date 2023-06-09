/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ISEARCH_BS_KAFKAREADERWRAPPER_H
#define ISEARCH_BS_KAFKAREADERWRAPPER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <librdkafka/rdkafkacpp.h>
#include "KafkaSinglePartitionConsumerWrapper.h"
#include "KafkaClientErrorCode.h"

namespace pluginplatform {
namespace reader_plugins {

struct KafkaReaderParam {
    std::string topicName;
    build_service::proto::Range range;
};

class KafkaReaderWrapper
{
public:
    KafkaReaderWrapper();
    virtual ~KafkaReaderWrapper();
private:
    KafkaReaderWrapper(const KafkaReaderWrapper &);
    KafkaReaderWrapper& operator=(const KafkaReaderWrapper &);
public:
    bool init(KafkaReaderParam param, const build_service::KeyValueMap &kvMap);
    void setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp);
    RdKafka::KafkaClientErrorCode seekByTimestamp(int64_t timestamp);
    virtual RdKafka::KafkaClientErrorCode read(int64_t &timestamp,
                            RdKafka::Message &message,
                            int64_t timeout = 3 * 1000000);
    virtual RdKafka::KafkaClientErrorCode read(int64_t &timestamp,
                            std::shared_ptr<RdKafka::Message> &message,
                            int64_t timeout = 3 * 1000000);
    virtual RdKafka::KafkaClientErrorCode read(int64_t &timestamp,
                            std::vector<std::shared_ptr<RdKafka::Message>> &messages,
                            int64_t timeout = 3 * 1000000);
private:
    RdKafka::KafkaClientErrorCode doBatchRead(int64_t &timestamp,
            std::vector<std::shared_ptr<RdKafka::Message>> &messages,
            int64_t timeout);
    bool tryRead(std::vector<std::shared_ptr<RdKafka::Message>> &messages);
    bool tryFillBuffer();
    RdKafka::KafkaClientErrorCode fillBuffer(int64_t timeout);
    std::shared_ptr<RdKafka::Conf> getGlobalConf(const build_service::KeyValueMap &kvMap);
    std::shared_ptr<RdKafka::Conf> getTopicConf(const build_service::KeyValueMap &kvMap);
    std::shared_ptr<RdKafka::Metadata> getTopicMeta();
    std::vector<std::pair<int32_t, build_service::proto::Range>> getPartRangeVec(
            build_service::proto::Range range,
            size_t kafkaPartCount);
    int64_t getNextMsgTimestamp() const;
    KafkaSinglePartitionConsumerConfig getSinglePartConsumerConfig();
    RdKafka::KafkaClientErrorCode reportFatalError();
    RdKafka::KafkaClientErrorCode mergeErrorCode(RdKafka::KafkaClientErrorCode ec);
    RdKafka::KafkaClientErrorCode checkTimestampLimit();
private:
    // for test
    virtual RdKafka::Consumer* createConsumer(
            std::shared_ptr<RdKafka::Conf> &conf, std::string &errStr);
    virtual RdKafka::Topic* createTopic(
            std::shared_ptr<RdKafka::Consumer> &consumer, std::string &topicName,
            std::shared_ptr<RdKafka::Conf> &conf, std::string &errStr);
private:
    std::shared_ptr<RdKafka::Consumer> _consumer;
    std::vector<KafkaSinglePartitionConsumerWrapperPtr> _partConsumersVec;
    std::shared_ptr<RdKafka::Topic> _topic;
    std::shared_ptr<RdKafka::Conf> _globalConf;
    std::shared_ptr<RdKafka::Conf> _topicConf;
    std::shared_ptr<RdKafka::Metadata> _topicMeta;
    size_t _kafkaPartCount;
    std::vector<std::pair<int32_t, build_service::proto::Range>> _partRangeVec;
    std::vector<std::shared_ptr<RdKafka::Message>> _messageBuffer;
    size_t _bufferCursor;
    int64_t _curTimestamp;
    bool _fillAllPart;
    uint32_t _lastReadIndex;
    bool _timeLimitMode;
    std::vector<bool> _exceedLimitVec;
private:
    BS_LOG_DECLARE();
};

typedef std::shared_ptr<KafkaReaderWrapper> KafkaReaderWrapperPtr;

}
}

#endif //ISEARCH_BS_KAFKAREADERWRAPPER_H
