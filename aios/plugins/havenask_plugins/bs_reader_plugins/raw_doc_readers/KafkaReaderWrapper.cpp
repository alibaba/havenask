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
#include "KafkaReaderWrapper.h"

using namespace std;
using namespace build_service;

namespace pluginplatform {
namespace reader_plugins {
BS_LOG_SETUP(reader, KafkaReaderWrapper);

static const string KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
static const string KAFKA_ENABLE_PARTITION_EOF = "enable.partition.eof";

static const int64_t MAX_TIME_STAMP = numeric_limits<int64_t>::max();
static const int64_t DEFAULT_TIMEOUT_MS = 3000;
static const uint16_t DEFAULT_RANGE_FROM = 0;
static const uint16_t DEFAULT_RANGE_TO = 65535;

static const uint64_t BATCH_READ_COUNT = 256;
static const uint64_t READ_BUFFER_SIZE = 32767;
static const uint64_t BATCH_CONSUME_COUNT = 2048;

KafkaReaderWrapper::KafkaReaderWrapper()
    : _kafkaPartCount(0), _bufferCursor(0), _curTimestamp(-1)
    , _fillAllPart(false), _lastReadIndex(-1), _timeLimitMode(false)
{}

KafkaReaderWrapper::~KafkaReaderWrapper() {
    for (auto &partConsumer : _partConsumersVec) {
        RdKafka::ErrorCode ec = partConsumer->stop();
        if (RdKafka::ERR_NO_ERROR != ec) {
            BS_LOG(ERROR, "failed to stop consumer.part[%d]: [%s]",
                   partConsumer->partition(), RdKafka::err2str(ec).c_str());
        }
    }
}

bool KafkaReaderWrapper::init(KafkaReaderParam param, const KeyValueMap &kvMap) {
    string errStr;
    _globalConf = getGlobalConf(kvMap);
    _topicConf = getTopicConf(kvMap);

    _consumer.reset(createConsumer(_globalConf, errStr));
    if (!_consumer) {
        BS_LOG(ERROR, "failed to create consumer: [%s]", errStr.c_str());
        return false;
    }
    _topic.reset(createTopic(_consumer, param.topicName, _topicConf, errStr));
    if (!_topic) {
        BS_LOG(ERROR, "failed to create topic: [%s]", errStr.c_str());
        return false;
    }
    _topicMeta = getTopicMeta();
    if (!_topicMeta) {
        BS_LOG(ERROR, "failed to get topic meta for [%s]", param.topicName.c_str());
        return false;
    }
    _kafkaPartCount = _topicMeta->topics()->front()->partitions()->size();
    _partRangeVec = getPartRangeVec(param.range, _kafkaPartCount);
    KafkaSinglePartitionConsumerConfig singlePartConfig = getSinglePartConsumerConfig();
    for (const auto &partRange : _partRangeVec) {
        _partConsumersVec.emplace_back(new KafkaSinglePartitionConsumerWrapper(
                        singlePartConfig, _consumer, _topic, partRange.first,
                        partRange.second));
    }
    _exceedLimitVec.resize(_partConsumersVec.size(), false);
    for (auto &partConsumer : _partConsumersVec) {
        RdKafka::ErrorCode ec = partConsumer->start(_topic,
                RdKafka::Topic::OFFSET_BEGINNING);
        if (RdKafka::ERR_NO_ERROR != ec) {
            BS_LOG(ERROR, "failed to start consumer: [%s]", RdKafka::err2str(ec).c_str());
            return false;
        } else {
            BS_LOG(INFO, "start consumer: part[%d], range[%d:%d]",
                   partConsumer->partition(), partConsumer->range().from(),
                   partConsumer->range().to());
        }
    }
    BS_LOG(INFO, "kafka reader init succuss, topic[%s], parts[%ld], range[%d:%d]",
           param.topicName.c_str(), _kafkaPartCount,
           param.range.from(), param.range.to());
    return true;
}

RdKafka::KafkaClientErrorCode KafkaReaderWrapper::read(int64_t &timestamp,
        RdKafka::Message &message, int64_t timeout)
{
    if (_bufferCursor >= _messageBuffer.size()) {
        _messageBuffer.clear();
        _bufferCursor = 0;
        RdKafka::KafkaClientErrorCode ec = doBatchRead(timestamp, _messageBuffer, timeout);
        if (RdKafka::ERR_NO_ERROR != ec) {
            return ec;
        }
    }
    if (_bufferCursor >= _messageBuffer.size()) {
        return RdKafka::ERR__PARTITION_EOF;
    }
    message = *_messageBuffer[_bufferCursor];
    _bufferCursor++;
    timestamp = _curTimestamp;
    return RdKafka::ERR_NO_ERROR;
}

RdKafka::KafkaClientErrorCode KafkaReaderWrapper::read(int64_t &timestamp,
        std::shared_ptr<RdKafka::Message> &message,
        int64_t timeout)
{
    if (_bufferCursor >= _messageBuffer.size()) {
        _messageBuffer.clear();
        _bufferCursor = 0;
        RdKafka::KafkaClientErrorCode ec = doBatchRead(timestamp, _messageBuffer, timeout);
        if (RdKafka::ERR_NO_ERROR != ec) {
            return ec;
        }
    }
    if (_bufferCursor >= _messageBuffer.size()) {
        return RdKafka::ERR__PARTITION_EOF;
    }
    _messageBuffer[_bufferCursor].swap(message);
    _bufferCursor++;
    timestamp = _curTimestamp;
    return RdKafka::ERR_NO_ERROR;
}

RdKafka::KafkaClientErrorCode KafkaReaderWrapper::read(int64_t &timestamp,
                        std::vector<std::shared_ptr<RdKafka::Message>> &messages,
                        int64_t timeout)
{
    if (_bufferCursor < _messageBuffer.size()) {
        messages.clear();
        for (;_bufferCursor < _messageBuffer.size(); ++_bufferCursor) {
            std::shared_ptr<RdKafka::Message> msg;
            _messageBuffer[_bufferCursor].swap(msg);
            messages.push_back(msg);
        }
        timestamp = _curTimestamp;
        return RdKafka::ERR_NO_ERROR;
    }
    return doBatchRead(timestamp, messages, timeout);
}

RdKafka::KafkaClientErrorCode KafkaReaderWrapper::doBatchRead(int64_t &timestamp,
        std::vector<std::shared_ptr<RdKafka::Message>> &messages,
        int64_t timeout)
{
    RdKafka::KafkaClientErrorCode ec = reportFatalError();
    if (ec != RdKafka::ERR_NO_ERROR) {
        timestamp = _curTimestamp;
        return ec;
    }
    RdKafka::KafkaClientErrorCode checkLimitEc = checkTimestampLimit();
    if (checkLimitEc != RdKafka::ERR_NO_ERROR) {
        timestamp = _curTimestamp;
        return checkLimitEc;
    }
    if (tryRead(messages)) {
        // tryFillBuffer();
        timestamp = _curTimestamp;
        return RdKafka::ERR_NO_ERROR;
    }
    ec = fillBuffer(timeout);
    checkLimitEc = checkTimestampLimit();
    if (checkLimitEc != RdKafka::ERR_NO_ERROR) {
        timestamp = _curTimestamp;
        return checkLimitEc;
    }
    if (tryRead(messages)) {
        timestamp = _curTimestamp;
        return RdKafka::ERR_NO_ERROR;
    }
    return mergeErrorCode(ec);
}

bool KafkaReaderWrapper::tryRead(std::vector<std::shared_ptr<RdKafka::Message>> &messages) {
    _lastReadIndex = -1;
    _fillAllPart = false;
    int64_t oldestPartTimestamp = MAX_TIME_STAMP;
    size_t readIndex = 0;
    for(size_t i = 0; i < _partConsumersVec.size(); ++i) {
        int64_t t = _partConsumersVec[i]->getFirstMsgTimestamp();
        if (-1 == t) {
            _fillAllPart = true;
            continue;
        }
        if (t < oldestPartTimestamp) {
            oldestPartTimestamp = t;
            readIndex = i;
        }
    }
    bool ret = (oldestPartTimestamp != MAX_TIME_STAMP)
               && _partConsumersVec[readIndex]->tryRead(messages);
    _curTimestamp = getNextMsgTimestamp();
    _lastReadIndex = readIndex;
    return ret;
}

bool KafkaReaderWrapper::tryFillBuffer() {
    if (!_fillAllPart) {
        return _partConsumersVec[_lastReadIndex]->tryFillBuffer();
    }
    for(size_t i = 0; i < _partConsumersVec.size(); ++i) {
        _partConsumersVec[i]->tryFillBuffer();
    }
    return true;
}

RdKafka::KafkaClientErrorCode KafkaReaderWrapper::fillBuffer(int64_t timeout) {
    tryFillBuffer();
    return RdKafka::ERR_NO_ERROR;
}

RdKafka::KafkaClientErrorCode KafkaReaderWrapper::seekByTimestamp(int64_t timestamp) {
    // clear buffer
    _messageBuffer.clear();
    _bufferCursor = 0;
    // seek
    std::vector<RdKafka::TopicPartition *> topicPartitionVec;
    for (auto &partConsumer : _partConsumersVec) {
        topicPartitionVec.push_back(RdKafka::TopicPartition::create(
                        _topic->name(), partConsumer->partition(), timestamp));
    }
    RdKafka::ErrorCode ec = _consumer->offsetsForTimes(topicPartitionVec, DEFAULT_TIMEOUT_MS);
    if (ec != RdKafka::ERR_NO_ERROR) {
        RdKafka::TopicPartition::destroy(topicPartitionVec);
        return ec;
    }
    RdKafka::ErrorCode rec = RdKafka::ERR_NO_ERROR;
    for (size_t i = 0; i < _partConsumersVec.size(); ++i) {
        RdKafka::ErrorCode ec = _partConsumersVec[i]->seek(topicPartitionVec[i]->offset(),
                timestamp, DEFAULT_TIMEOUT_MS);
        if (ec != RdKafka::ERR_NO_ERROR) {
            rec = ec;
        }
    }
    if (ec == RdKafka::ERR_NO_ERROR) {
        _curTimestamp = getNextMsgTimestamp();
    }
    RdKafka::TopicPartition::destroy(topicPartitionVec);
    return rec;
}

std::shared_ptr<RdKafka::Conf> KafkaReaderWrapper::getGlobalConf(const KeyValueMap &kvMap) {
    std::shared_ptr<RdKafka::Conf> globalConf(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    if (!globalConf) {
        BS_LOG(ERROR, "failed to create global config");
        return nullptr;
    }
    string brokers;
    KeyValueMap::const_iterator it = kvMap.find(KAFKA_BOOTSTRAP_SERVERS);
    if (it != kvMap.end()) {
        brokers = it->second;
    }
    string errStr;
    if (RdKafka::Conf::CONF_OK != globalConf->set(KAFKA_BOOTSTRAP_SERVERS, brokers, errStr)) {
        BS_LOG(ERROR, "failed to set conf [%s]: [%s]",
               KAFKA_BOOTSTRAP_SERVERS.c_str(), errStr.c_str());
    }
    if (RdKafka::Conf::CONF_OK != globalConf->set(KAFKA_ENABLE_PARTITION_EOF, "true", errStr)) {
        BS_LOG(ERROR, "failed to set conf [%s]: [%s]",
               KAFKA_BOOTSTRAP_SERVERS.c_str(), errStr.c_str());
    }

    return globalConf;
}

std::shared_ptr<RdKafka::Conf> KafkaReaderWrapper::getTopicConf(const KeyValueMap &kvMap) {
    std::shared_ptr<RdKafka::Conf> topicConf(
            RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
    if (!topicConf) {
        BS_LOG(ERROR, "failed to create topic config");
        return nullptr;
    }
    return topicConf;
}

std::shared_ptr<RdKafka::Metadata> KafkaReaderWrapper::getTopicMeta() {
    class RdKafka::Metadata *metadata;
    RdKafka::ErrorCode ec = _consumer->metadata(false,
            _topic.get(), &metadata, DEFAULT_TIMEOUT_MS);
    std::shared_ptr<RdKafka::Metadata> topicMeta(metadata);
    if (ec != RdKafka::ERR_NO_ERROR || topicMeta->topics()->size() != 1) {
        return std::shared_ptr<RdKafka::Metadata>();
    }
    return topicMeta;
}

std::vector<std::pair<int32_t, proto::Range>> KafkaReaderWrapper::getPartRangeVec(
        proto::Range range, size_t kafkaPartCount)
{
    std::vector<std::pair<int32_t, proto::Range>> partRangeVec;
    if (kafkaPartCount == 0) {
        return partRangeVec;
    }
    uint32_t rangeCount = DEFAULT_RANGE_TO - DEFAULT_RANGE_FROM + 1;
    uint32_t rangeFrom = DEFAULT_RANGE_FROM;
    uint32_t rangeTo = DEFAULT_RANGE_TO;
    uint32_t c = rangeCount / kafkaPartCount;
    uint32_t m = rangeCount % kafkaPartCount;
    uint32_t from = rangeFrom;
    for (uint32_t i = 0; i < kafkaPartCount && from <= rangeTo; ++i) {
        uint32_t to = from + c + (i >= m ? 0 : 1) - 1;
        if (!((from > range.to()) || (range.from() > to))) {
            proto::Range partRange;
            partRange.set_from(std::max(from, range.from()));
            partRange.set_to(std::min(to, range.to()));
            partRangeVec.push_back(std::make_pair(i, partRange));
        }
        from = to + 1;
    }
    return partRangeVec;
}

int64_t KafkaReaderWrapper::getNextMsgTimestamp() const {
    int64_t timestamp = MAX_TIME_STAMP;
    for (auto &partConsumer : _partConsumersVec) {
        int64_t ts = partConsumer->getNextMsgTimestamp();
        if (ts < timestamp) {
            timestamp = ts;
        }
    }
    return timestamp;
}

KafkaSinglePartitionConsumerConfig KafkaReaderWrapper::getSinglePartConsumerConfig() {
    KafkaSinglePartitionConsumerConfig config;
    config.batchReadCount = BATCH_READ_COUNT;
    config.batchConsumeCount = BATCH_CONSUME_COUNT;
    config.readBufferSize = READ_BUFFER_SIZE;
    return config;
}

RdKafka::KafkaClientErrorCode KafkaReaderWrapper::reportFatalError() {
    RdKafka::ErrorCode ec = RdKafka::ERR_NO_ERROR;
    for (auto &partConsumer : _partConsumersVec) {
        ec = partConsumer->reportFatalError();
        if (ec != RdKafka::ERR_NO_ERROR) {
            return ec;
        }
    }
    return ec;
}

RdKafka::KafkaClientErrorCode KafkaReaderWrapper::mergeErrorCode(
        RdKafka::KafkaClientErrorCode ec)
{
    if (ec != RdKafka::ERR__PARTITION_EOF) {
        return ec;
    }
    return RdKafka::ERR_NO_ERROR;
}

RdKafka::KafkaClientErrorCode KafkaReaderWrapper::checkTimestampLimit() {
    if (_timeLimitMode) {
        size_t exceedCount = 0;
        for (size_t i = 0; i < _partConsumersVec.size(); ++i) {
            if (!_exceedLimitVec[i]) {
                _exceedLimitVec[i] = _partConsumersVec[i]->exceedTimestampLimit();
            }
            if (_exceedLimitVec[i]) {
                exceedCount++;
            }
        }
        BS_LOG(DEBUG, "kafka timestamp limit check:[%lu/%lu]", exceedCount,
               _partConsumersVec.size());
        if (exceedCount == _partConsumersVec.size()) {
            return RdKafka::ERR_EX_CLIENT_EXCEED_TIME_STAMP_LIMIT;
        }
    }
    return RdKafka::ERR_NO_ERROR;
}

void KafkaReaderWrapper::setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) {
    if (timestampLimit != MAX_TIME_STAMP) {
        _timeLimitMode = true;
        int64_t maxAcceptTimestamp = timestampLimit;
        for (size_t i = 0; i < _partConsumersVec.size(); i++) {
            int64_t lastMsgTime = _partConsumersVec[i]->getLastMsgTimestamp();
            maxAcceptTimestamp = std::max(maxAcceptTimestamp, lastMsgTime);
        }
        for (size_t i = 0; i < _partConsumersVec.size(); i++) {
            _partConsumersVec[i]->setTimestampLimit(maxAcceptTimestamp);
        }
        for (size_t i = 0; i < _partConsumersVec.size(); i++) {
            int64_t nextTimestamp = _partConsumersVec[i]->getNextMsgTimestamp();
            if (nextTimestamp == -1 || nextTimestamp <= maxAcceptTimestamp) {
                _exceedLimitVec[i] = false;
            }
        }
        acceptTimestamp = maxAcceptTimestamp;
    } else {
        _timeLimitMode = false;
        for (size_t i = 0; i < _partConsumersVec.size(); i++) {
            _partConsumersVec[i]->setTimestampLimit(MAX_TIME_STAMP);
            _exceedLimitVec[i] = false;
        }
        acceptTimestamp = MAX_TIME_STAMP;
    }
}

RdKafka::Consumer* KafkaReaderWrapper::createConsumer(
        std::shared_ptr<RdKafka::Conf> &conf, std::string &errStr)
{
    return RdKafka::Consumer::create(conf.get(), errStr);
}

RdKafka::Topic* KafkaReaderWrapper::createTopic(
        std::shared_ptr<RdKafka::Consumer> &consumer, std::string &topicName,
        std::shared_ptr<RdKafka::Conf> &conf, std::string &errStr)
{
    return RdKafka::Topic::create(consumer.get(), topicName, conf.get(), errStr);
}

}
}
