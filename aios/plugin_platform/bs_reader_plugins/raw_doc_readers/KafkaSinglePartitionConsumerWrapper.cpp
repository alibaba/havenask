#include <autil/HashAlgorithm.h>
#include "KafkaSinglePartitionConsumerWrapper.h"

using namespace std;
using namespace autil;
using namespace build_service;

namespace pluginplatform {
namespace reader_plugins {
BS_LOG_SETUP(reader, KafkaSinglePartitionConsumerWrapper);

static const uint32_t HASH_SIZE = 65536;
static const int64_t DEFAULT_CONSUME_TIMEOUT_MS = 1000;
static const int64_t MAX_TIME_STAMP = numeric_limits<int64_t>::max();

KafkaSinglePartitionConsumerWrapper::KafkaSinglePartitionConsumerWrapper(
        const KafkaSinglePartitionConsumerConfig &config,
        std::shared_ptr<RdKafka::Consumer> &consumer,
        std::shared_ptr<RdKafka::Topic> &topic,
        int32_t partition,
        proto::Range range)
    : _config(config), _consumer(consumer), _topic(topic)
    , _partition(partition), _range(range), _bufferCursor(0)
    , _lastMsgTimestamp(-1), _seekTimestamp(-1)
    , _lastErrorCode(RdKafka::ERR_NO_ERROR), _timestampLimit(MAX_TIME_STAMP)
{
}

KafkaSinglePartitionConsumerWrapper::~KafkaSinglePartitionConsumerWrapper() {
}

int64_t KafkaSinglePartitionConsumerWrapper::getFirstMsgTimestamp() const {
    if (_bufferCursor < _buffer.size()) {
        return _buffer[_bufferCursor]->timestamp().timestamp;
    }
    return -1;
}

int64_t KafkaSinglePartitionConsumerWrapper::getNextMsgTimestamp() const {
    int64_t t = getFirstMsgTimestamp();
    if (t != -1) {
        return t;
    }
    return std::max(_lastMsgTimestamp, _seekTimestamp);
}

int64_t KafkaSinglePartitionConsumerWrapper::getLastMsgTimestamp() const {
    return _lastMsgTimestamp;
}

bool KafkaSinglePartitionConsumerWrapper::tryRead(
        std::vector<std::shared_ptr<RdKafka::Message>> &messages)
{
    messages.clear();
    int32_t batchReadCount = _config.batchReadCount;
    while(batchReadCount--) {
        if (getNextMsgTimestamp() > _timestampLimit) {
            break;
        }
        std::shared_ptr<RdKafka::Message> msg;
        if (_bufferCursor < _buffer.size()) {
            _buffer[_bufferCursor++].swap(msg);
            messages.push_back(msg);
            _lastMsgTimestamp = msg->timestamp().timestamp;
        } else {
            break;
        }
    }
    return messages.size() > 0;
}

bool KafkaSinglePartitionConsumerWrapper::tryFillBuffer() {
    std::vector<std::shared_ptr<RdKafka::Message>> messages;
    if (getNextMsgTimestamp() > _timestampLimit) {
        return false;
    }
    RdKafka::ErrorCode ec = consumeBatch(messages);
    checkErrorCode(ec);
    if (_buffer.size() + messages.size() <= _config.readBufferSize) {
        _buffer.insert(_buffer.end(), messages.begin(), messages.end());
    } else {
        messages.insert(messages.begin(), _buffer.begin() + _bufferCursor, _buffer.end());
        _buffer.swap(messages);
    }
    return true;
}

RdKafka::ErrorCode KafkaSinglePartitionConsumerWrapper::consumeOne(std::shared_ptr<RdKafka::Message> &message) {
    std::shared_ptr<RdKafka::Message> msg(
            _consumer->consume(_topic.get(), _partition, DEFAULT_CONSUME_TIMEOUT_MS));
    if (!msg) {
        return RdKafka::ERR_UNKNOWN;
    }
    _consumer->poll(0);
    RdKafka::ErrorCode ec = msg->err();
    if (RdKafka::ERR_NO_ERROR != ec) {
        if (RdKafka::ERR__TIMED_OUT == ec) {
            return RdKafka::ERR__PARTITION_EOF;
        }
        return ec;
    }
    BS_LOG(DEBUG, "message info: part=[%d], ec=[%d|%s], key=[%s], "
           "payload=[%s], timestamp=[%ld]", _partition, ec,
           RdKafka::err2str(ec).c_str(), (msg->key()? msg->key()->c_str(): "NULL"),
           static_cast<const char*>(msg->payload()), msg->timestamp().timestamp);
    if (!rangeFilter(msg)) {
        BS_LOG(DEBUG, "message filted");
        return RdKafka::ERR_INVALID_MSG;
    }
    message.swap(msg);
    return RdKafka::ERR_NO_ERROR;
}

RdKafka::ErrorCode KafkaSinglePartitionConsumerWrapper::consumeBatch(
        std::vector<std::shared_ptr<RdKafka::Message>> &messages) {
    RdKafka::ErrorCode rec = RdKafka::ERR_NO_ERROR;
    for (size_t i = 0; i < _config.batchConsumeCount; ++i) {
        std::shared_ptr<RdKafka::Message> msg;
        RdKafka::ErrorCode ec = consumeOne(msg);
        if (RdKafka::ERR_NO_ERROR != ec) {
            if (ec != RdKafka::ERR_INVALID_MSG) {
                rec = ec;
                break;
            } else {
                continue;
            }
        }
        messages.push_back(msg);
    }
    BS_LOG(DEBUG, "batch msgs.size: [%lu]", messages.size());
    return rec;
}

bool KafkaSinglePartitionConsumerWrapper::rangeFilter(
        std::shared_ptr<RdKafka::Message> &message)
{
    if (!message || !message->key()) {
        return false;
    }
    uint32_t hashId = hashFunc(*message->key());
    BS_LOG(DEBUG, "message key hashId: [%d]", hashId);
    return _range.from() <= hashId && hashId <= _range.to();
}

RdKafka::ErrorCode KafkaSinglePartitionConsumerWrapper::seek(
        int64_t offset, int64_t timestamp, int timeout)
{
    RdKafka::ErrorCode ec = _consumer->seek(_topic.get(), _partition, offset, timeout);
    if (RdKafka::ERR_NO_ERROR == ec) {
        clear();
        _lastMsgTimestamp = -1;
        _seekTimestamp = timestamp;
    }
    checkErrorCode(ec);
    return ec;
}

RdKafka::ErrorCode KafkaSinglePartitionConsumerWrapper::start(
        std::shared_ptr<RdKafka::Topic> &topic, int64_t offset)
{
    RdKafka::ErrorCode ec = _consumer->start(topic.get(), _partition, offset);
    checkErrorCode(ec);
    return ec;
}

RdKafka::ErrorCode KafkaSinglePartitionConsumerWrapper::stop()
{
    RdKafka::ErrorCode ec = _consumer->stop(_topic.get(), _partition);
    checkErrorCode(ec);
    return ec;
}

void KafkaSinglePartitionConsumerWrapper::clear() {
    _buffer.clear();
    _bufferCursor = 0;
}

uint32_t KafkaSinglePartitionConsumerWrapper::hashFunc(const string &key) {
    // same with producer hash function, range: [0,65535]
    uint32_t h = (uint32_t)HashAlgorithm::hashString(key.c_str(), key.length(), 0);
    return h % HASH_SIZE;
}

RdKafka::ErrorCode KafkaSinglePartitionConsumerWrapper::reportFatalError() {
    RdKafka::ErrorCode ec = _lastErrorCode;
    bool isFatalError = (_lastErrorCode != RdKafka::ERR_NO_ERROR)
                        && (_lastErrorCode != RdKafka::ERR__PARTITION_EOF);
    if (isFatalError) {
        _lastErrorCode = RdKafka::ERR_NO_ERROR;
        return ec;
    }
    return RdKafka::ERR_NO_ERROR;
}

void KafkaSinglePartitionConsumerWrapper::checkErrorCode(RdKafka::ErrorCode ec)
{
    if (RdKafka::ERR_NO_ERROR != ec) {
        _lastErrorCode = ec;
    }
}

bool KafkaSinglePartitionConsumerWrapper::exceedTimestampLimit() {
    if (_timestampLimit == MAX_TIME_STAMP) {
        return false;
    }
    int64_t timestamp = getNextMsgTimestamp();
    if (timestamp == -1) {
        return false;
    }
    return timestamp > _timestampLimit;
}

void KafkaSinglePartitionConsumerWrapper::setTimestampLimit(int64_t timestampLimit) {
    _timestampLimit = timestampLimit;
}

}
}
