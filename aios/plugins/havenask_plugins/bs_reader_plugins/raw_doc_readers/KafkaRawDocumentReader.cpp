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
#include "KafkaRawDocumentReader.h"
#include "build_service/reader/ParserCreator.h"
#include "build_service/config/CLIOptionNames.h"
#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>
#include <autil/HashAlgorithm.h>
#include <indexlib/util/counter/StateCounter.h>

using namespace std;
using namespace autil;
using namespace build_service;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::reader;

namespace pluginplatform {
namespace reader_plugins {

static const std::string KAFKA_TOPIC_NAME = "topic_name";
static const std::string KAFKA_START_TIMESTAMP = "kafka_start_timestamp";
static const std::string KAFKA_STOP_TIMESTAMP = "kafka_stop_timestamp";
static const std::string KAFKA_SUSPEND_TIMESTAMP = "kafka_suspend_timestamp";

BS_LOG_SETUP(reader, KafkaRawDocumentReader);

KafkaRawDocumentReader::KafkaRawDocumentReader()
    : _lastDocTimestamp(0)
    , _startTimestamp(0)
    , _exceedTsAction(ExceedTsAction::ETA_STOP)
    , _exceedSuspendTimestamp(false)
{
}

KafkaRawDocumentReader::~KafkaRawDocumentReader() {

}

bool KafkaRawDocumentReader::init(const ReaderInitParam &params) {
    if (!RawDocumentReader::init(params)) {
        return false;
    }
    _kafkaReader.reset(new KafkaReaderWrapper);
    if (!_kafkaReader) {
        BS_LOG(ERROR, "create kafka reader failed");
        return false;
    }
    _kafkaReadDelayCounter = GET_STATE_COUNTER(params.counterMap, processor.kafkaReadDelay);
    if (!_kafkaReadDelayCounter) {
        BS_LOG(ERROR, "can not get kafkaReadDelay from counterMap");
    }
    KafkaReaderParam readerParam;
    readerParam.topicName = getValueFromKeyValueMap(params.kvMap, KAFKA_TOPIC_NAME);
    readerParam.range = params.range;
    if (!_kafkaReader->init(readerParam, params.kvMap)) {
        BS_LOG(ERROR, "kafka reader init failed");
        return false;
    }
    return initKafkaReader(_kafkaReader, params.kvMap);
}

bool KafkaRawDocumentReader::initKafkaReader(
        KafkaReaderWrapperPtr &kafkaReader, const KeyValueMap &kvMap)
{
    int64_t startTimestamp = 0;
    KeyValueMap::const_iterator it = kvMap.find(KAFKA_START_TIMESTAMP);
    if (it != kvMap.end() && !StringUtil::fromString<int64_t>(it->second, startTimestamp)) {
        BS_LOG(ERROR, "invalid startTimestamp[%s]", it->second.c_str());
        return false;
    }

    int64_t checkpoint = 0;
    it = kvMap.find(CHECKPOINT);
    if (it != kvMap.end() && !StringUtil::fromString<int64_t>(it->second, checkpoint)) {
        BS_LOG(ERROR, "invalid checkpoint[%s]", it->second.c_str());
        return false;
    }
    startTimestamp = std::max(startTimestamp, checkpoint);
    if (startTimestamp > 0 && !seek(startTimestamp)) {
        BS_LOG(ERROR, "kafka reader seek to %ld failed", startTimestamp);
        return false;
    }

    if (startTimestamp > 0) {
        _startTimestamp = startTimestamp;
        BS_LOG(INFO, "kafka reader seek to [%ld] at startup", _startTimestamp);
    }

    auto it1 = kvMap.find(KAFKA_SUSPEND_TIMESTAMP);
    auto it2 = kvMap.find(KAFKA_STOP_TIMESTAMP);

    if (it1 != kvMap.end() && it2 != kvMap.end()) {
        BS_LOG(ERROR, "cannot specify %s and %s at the same time",
               KAFKA_SUSPEND_TIMESTAMP.c_str(), KAFKA_STOP_TIMESTAMP.c_str());
        return false;
    }

    return setLimitTime(kafkaReader, kvMap, KAFKA_SUSPEND_TIMESTAMP) &&
        setLimitTime(kafkaReader, kvMap, KAFKA_STOP_TIMESTAMP);
}

bool KafkaRawDocumentReader::setLimitTime(
    KafkaReaderWrapperPtr &kafkaReader,
    const KeyValueMap &kvMap, const string &limitTimeType)
{
    auto it = kvMap.find(limitTimeType);
    if (it == kvMap.end()) {
        return true;
    }

    string msg;
    int64_t limitTime;
    if (!StringUtil::fromString(it->second, limitTime)) {
        BS_LOG(ERROR, "invalid %s[%s]", limitTimeType.c_str(), it->second.c_str());
        return false;
    }

    if (_startTimestamp > limitTime) {
        BS_LOG(ERROR, "startTimestamp[%ld] > %s[%ld]",
               _startTimestamp, limitTimeType.c_str(), limitTime);
        return false;
    }

    if (limitTime != numeric_limits<int64_t>::max()) {
        kafkaReader->setTimestampLimit(limitTime / 1000, limitTime);
        limitTime *= 1000;
        BS_LOG(INFO, "kafka reader will suspend at timestamp[%ld]", limitTime);
    }
    _exceedTsAction = (limitTimeType == KAFKA_SUSPEND_TIMESTAMP) ?
        ExceedTsAction::ETA_SUSPEND : ExceedTsAction::ETA_STOP;
    return true;
}

bool KafkaRawDocumentReader::seek(int64_t offset) {
    assert(_kafkaReader);
    BS_LOG(INFO, "kafka seek timestamp[%ld]", offset);
    RdKafka::KafkaClientErrorCode ec = _kafkaReader->seekByTimestamp(offset / 1000);
    if (ec != RdKafka::ERR_NO_ERROR
        && ec != RdKafka::ERR__PARTITION_EOF)
    {
        BS_LOG(ERROR, "seek failed for kafka source with [%ld]", offset);
        return false;
    }
    return true;
}

bool KafkaRawDocumentReader::seek(const Checkpoint& checkpoint) {
    assert(_kafkaReader);
    BS_LOG(INFO, "kafka seek timestamp[%ld]", checkpoint.offset);
    RdKafka::KafkaClientErrorCode ec = _kafkaReader->seekByTimestamp(checkpoint.offset / 1000);
    if (ec != RdKafka::ERR_NO_ERROR
        && ec != RdKafka::ERR__PARTITION_EOF)
    {
        BS_LOG(ERROR, "seek failed for kafka source with [%ld]", checkpoint.offset);
        return false;
    }
    return true;
}

bool KafkaRawDocumentReader::isEof() const {
    return false;
}

int64_t KafkaRawDocumentReader::getFreshness() {
    int64_t currentTime = TimeUtility::currentTime();
    int64_t freshness = std::max(currentTime - _lastDocTimestamp, 0L);
    return freshness;
}

void KafkaRawDocumentReader::suspendReadAtTimestamp(
    int64_t timestamp, ExceedTsAction action)
{
    autil::ScopedLock lock(_mutex);
    int64_t actualTimestamp = timestamp;
    _kafkaReader->setTimestampLimit(timestamp / 1000, actualTimestamp);
    actualTimestamp *= 1000;
    _exceedTsAction = action;
    _exceedSuspendTimestamp = false;
    BS_LOG(INFO, "kafka reader will suspend at [%ld]", actualTimestamp);
}

RawDocumentReader::ErrorCode KafkaRawDocumentReader::readDocStr(std::string& docStr, 
                                                                Checkpoint* checkpoint, 
                                                                DocInfo& docInfo)
{
    autil::ScopedLock lock(_mutex);
    std::shared_ptr<RdKafka::Message> message;
    RdKafka::KafkaClientErrorCode ec = _kafkaReader->read(
            checkpoint->offset, message, DEFAULT_WAIT_READER_TIME);
    if (RdKafka::ERR_NO_ERROR != ec) {
        if (RdKafka::ERR_EX_CLIENT_EXCEED_TIME_STAMP_LIMIT == ec) {
            if (_exceedTsAction == ExceedTsAction::ETA_SUSPEND) {
                BS_LOG(INFO, "kafka read exceed the limited timestamp, action: suspend");
                _exceedSuspendTimestamp = true;
                return ERROR_WAIT;
            } else {
                BS_LOG(INFO, "kafka read exceed the limited timestamp, action: eof");
                return ERROR_EOF;
            }
        } else if (RdKafka::ERR__PARTITION_EOF == ec) {
            return ERROR_WAIT;
        } else {
            BS_LOG(WARN, "read from kafka fail [ec = %d]", ec.code());
            return ERROR_WAIT;
        }
    }
    _lastDocTimestamp = checkpoint->offset * 1000;
    docStr = static_cast<const char *>(message->payload());
    docInfo.docTimestamp = message->timestamp().timestamp * 1000;
    _exceedSuspendTimestamp = false;

    int64_t delay = getFreshness() / (1000 * 1000);
    if (_kafkaReadDelayCounter) {
        _kafkaReadDelayCounter->Set(delay);
    }
    return ERROR_NONE;
}

RawDocumentReader::ErrorCode KafkaRawDocumentReader::readDocStr(std::vector<Message>& msgs, 
                                                                Checkpoint* checkpoint, 
                                                                size_t maxMessageCount)
{
    autil::ScopedLock lock(_mutex);
    std::vector<std::shared_ptr<RdKafka::Message>> messages;
    RdKafka::KafkaClientErrorCode ec = _kafkaReader->read(
            checkpoint->offset, messages, DEFAULT_WAIT_READER_TIME);
    if (RdKafka::ERR_NO_ERROR != ec) {
        if (RdKafka::ERR_EX_CLIENT_EXCEED_TIME_STAMP_LIMIT == ec) {
            if (_exceedTsAction == ExceedTsAction::ETA_SUSPEND) {
                BS_LOG(INFO, "kafka read exceed the limited timestamp, action: suspend");
                _exceedSuspendTimestamp = true;
                return ERROR_WAIT;
            } else {
                BS_LOG(INFO, "kafka read exceed the limited timestamp, action: eof");
                return ERROR_EOF;
            }
        } else if (RdKafka::ERR__PARTITION_EOF == ec) {
            return ERROR_WAIT;
        } else {
            BS_LOG(WARN, "read from kafka fail [ec = %d]", ec.code());
            return ERROR_WAIT;
        }
    }
    _lastDocTimestamp = checkpoint->offset * 1000;
    msgs.clear();
    size_t msgCnt = messages.size();
    msgs.reserve(msgCnt);
    for (size_t i = 0; i < msgCnt; ++i) {
        msgs.emplace_back();
        auto &msg = msgs.back();
        msg.data = static_cast<const char *>(messages[i]->payload());
        msg.timestamp = messages[i]->timestamp().timestamp * 1000;
    }
    _exceedSuspendTimestamp = false;

    int64_t delay = getFreshness() / (1000 * 1000);
    if (_kafkaReadDelayCounter) {
        _kafkaReadDelayCounter->Set(delay);
    }
    return ERROR_NONE;
}

bool KafkaRawDocumentReader::getMaxTimestampAfterStartTimestamp(int64_t &timestamp) {
    timestamp = TimeUtility::currentTime(); // TODO: seek to end to get accurate max timestamp
    return true;
}
}
}
