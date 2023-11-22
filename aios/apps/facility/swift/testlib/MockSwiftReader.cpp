#include "swift/testlib/MockSwiftReader.h"

#include <cstddef>
#include <functional>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <memory>
#include <type_traits>
#include <unistd.h>

#include "autil/TimeUtility.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {
namespace common {
class SchemaReader;
} // namespace common
} // namespace swift

using namespace std;
using namespace swift::protocol;
using namespace autil;

namespace swift {
namespace testlib {

AUTIL_LOG_SETUP(swift, MockSwiftReader);

MockSwiftReader::MockSwiftReader()
    : SwiftReaderImpl(nullptr, nullptr, protocol::TopicInfo())
    , _cursor(0)
    , _timestampLimit(-1)
    , _noMoreMessageTimestamp(-1)
    , _schemaReader(nullptr)
    , _maxSchemaDocCount(0) {
    ON_CALL(*this, read(_, _, _))
        .WillByDefault(Invoke(std::bind(
            &MockSwiftReader::readForTest, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)));
    ON_CALL(*this, readMulti(_, _, _))
        .WillByDefault(Invoke(std::bind(&MockSwiftReader::readMultiForTest,
                                        this,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        std::placeholders::_3)));
    ON_CALL(*this, init(_))
        .WillByDefault(Invoke(std::bind(&MockSwiftReader::initForTest, this, std::placeholders::_1)));
    ON_CALL(*this, seekByTimestamp(_, _))
        .WillByDefault(Invoke(
            std::bind(&MockSwiftReader::seekByTimestampForTest, this, std::placeholders::_1, std::placeholders::_2)));
    ON_CALL(*this, setTimestampLimit(_, _))
        .WillByDefault(Invoke(
            std::bind(&MockSwiftReader::setTimestampLimitForTest, this, std::placeholders::_1, std::placeholders::_2)));
    ON_CALL(*this, getSwiftPartitionStatus())
        .WillByDefault(Invoke(std::bind(&MockSwiftReader::getSwiftPartitionStatusForTest, this)));

    ON_CALL(*this, getSchemaReader(_, _))
        .WillByDefault(Invoke(
            std::bind(&MockSwiftReader::getSchemaReaderForTest, this, std::placeholders::_1, std::placeholders::_2)));
    ON_CALL(*this, getReaderProgress(_))
        .WillByDefault(Invoke(std::bind(&MockSwiftReader::getReaderProgressForTest, this, std::placeholders::_1)));
    ON_CALL(*this, seekByProgress(_, _))
        .WillByDefault(Invoke(
            std::bind(&MockSwiftReader::seekByProgressForTest, this, std::placeholders::_1, std::placeholders::_2)));
}

MockSwiftReader::~MockSwiftReader() {}

swift::common::SchemaReader *MockSwiftReader::getSchemaReaderForTest(const char *doc, ErrorCode &ec) {
    if (_schemaReader != nullptr && _cursor <= _maxSchemaDocCount) {
        ec = ERROR_NONE;
        return _schemaReader;
    }
    ec = ERROR_CLIENT_SCHEMA_MSG_INVALID;
    return nullptr;
}

ErrorCode MockSwiftReader::readForTest(int64_t &timestamp, Message &message, int64_t timeout) {
    usleep(200 * 1000);
    if (_cursor >= _messages.size()) {
        timestamp = TimeUtility::currentTimeInMicroSeconds();
        if (_timestampLimit != -1 && timestamp > _timestampLimit) {
            return ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT;
        }
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    if (_timestampLimit != -1 && _messages[_cursor].second > _timestampLimit) {
        return ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT;
    }
    MessageInfo &messageInfo = _messages[_cursor++];
    message = messageInfo.first;
    timestamp = messageInfo.second;
    return ERROR_NONE;
}

ErrorCode MockSwiftReader::readMultiForTest(int64_t &timestamp, Messages &messages, int64_t timeout) {
    usleep(200 * 1000);
    messages.clear_msgs();
    if (_cursor >= _messages.size()) {
        timestamp = TimeUtility::currentTimeInMicroSeconds();
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    for (; _cursor < _messages.size(); ++_cursor) {
        if (_timestampLimit != -1 && _messages[_cursor].second > _timestampLimit) {
            break;
        }
        messages.add_msgs()->Swap(&_messages[_cursor].first);
        timestamp = _messages[_cursor].second;
    }
    if (messages.msgs_size() == 0) {
        timestamp = TimeUtility::currentTimeInMicroSeconds();
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    return ERROR_NONE;
}

ErrorCode MockSwiftReader::seekByTimestampForTest(int64_t timestamp, bool force) {
    if (_cursor >= _messages.size()) {
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    size_t i = force ? 0 : _cursor;
    for (; i < _messages.size(); i++) {
        if (timestamp <= _messages[i].first.timestamp()) {
            break;
        }
    }
    _cursor = i;
    return ERROR_NONE;
}

bool MockSwiftReader::isEqual(const protocol::ReaderProgress &progress1, const protocol::ReaderProgress &progress2) {
    if (progress1.topicprogress().size() != progress2.topicprogress().size()) {
        return false;
    }
    for (size_t i = 0; i < progress1.topicprogress().size(); i++) {
        auto topicProgress1 = progress1.topicprogress(i);
        auto topicProgress2 = progress2.topicprogress(i);
        if (topicProgress1.topicname() != topicProgress2.topicname() ||
            topicProgress1.uint8filtermask() != topicProgress2.uint8filtermask() ||
            topicProgress1.uint8maskresult() != topicProgress2.uint8maskresult()) {
            return false;
        }
        if (topicProgress1.partprogress().size() != topicProgress2.partprogress().size()) {
            return false;
        }
        for (size_t j = 0; j < topicProgress1.partprogress().size(); j++) {
            if (topicProgress1.partprogress(j).from() != topicProgress2.partprogress(j).from() ||
                topicProgress1.partprogress(j).to() != topicProgress2.partprogress(j).to() ||
                topicProgress1.partprogress(j).timestamp() != topicProgress2.partprogress(j).timestamp()) {
                return false;
            }
        }
    }
    return true;
}

ErrorCode MockSwiftReader::seekByProgressForTest(const protocol::ReaderProgress &progress, bool force) {
    if (_cursor >= _progress.size()) {
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    size_t i = force ? 0 : _cursor;
    for (; i < _progress.size(); i++) {
        if (isEqual(progress, _progress[i])) {
            break;
        }
    }
    _cursor = i;
    return ERROR_NONE;
}

protocol::ErrorCode MockSwiftReader::getReaderProgressForTest(protocol::ReaderProgress &progress) {
    if (_cursor >= _progress.size()) {
        if (_cursor == 0) {
            progress = createProgress(0);
            return ERROR_NONE;
        }
        return ERROR_CLIENT_NO_MORE_MESSAGE;
    }
    for (const auto &topicProgress : _progress[_cursor].topicprogress()) {
        TopicReaderProgress *retProgress = progress.add_topicprogress();
        *retProgress = topicProgress;
    }
    return ERROR_NONE;
}

void MockSwiftReader::setTimestampLimitForTest(int64_t timestamp, int64_t &actualTimestamp) {
    _timestampLimit = timestamp;
    // if (!_messages.empty() &_messages.back().second > timestamp) {
    //     _timestampLimit = _messages.back().second;
    // }
    actualTimestamp = _timestampLimit;
}

void MockSwiftReader::addMessage(const string &messageData,
                                 int64_t timestamp,
                                 int64_t retTimestamp,
                                 DataType type,
                                 uint16_t msgU16payload,
                                 uint32_t offsetInRawMsg) {
    if (retTimestamp == -1) {
        retTimestamp = timestamp;
    }
    Message message;
    message.set_data(messageData);
    message.set_timestamp(timestamp);
    message.set_datatype(type);
    message.set_uint16payload(msgU16payload);
    message.set_uint8maskpayload(_uint8MaskResult);
    message.set_offsetinrawmsg(offsetInRawMsg);
    _messages.push_back(make_pair(message, retTimestamp));
    _progress.push_back(createProgress(timestamp + 1));
}

void MockSwiftReader::setNoMoreMessageAtTimestamp(int64_t timestamp) { _noMoreMessageTimestamp = timestamp; }

swift::client::SwiftPartitionStatus MockSwiftReader::getSwiftPartitionStatusForTest() const {
    swift::client::SwiftPartitionStatus status;
    status.refreshTime = 0;
    status.maxMessageId = 0;
    status.maxMessageTimestamp = 0;
    return status;
}

ErrorCode MockSwiftReader::initForTest(const client::SwiftReaderConfig &config) {
    _topicName = config.topicName;
    auto swiftFilter = config.swiftFilter;
    _from = swiftFilter.from();
    _to = swiftFilter.to();
    _uint8FilterMask = swiftFilter.uint8filtermask();
    _uint8MaskResult = swiftFilter.uint8maskresult();
    return ERROR_NONE;
}

swift::protocol::ReaderProgress MockSwiftReader::createProgress(int64_t timestamp) {
    swift::protocol::ReaderProgress progress;
    TopicReaderProgress *topicProgress = progress.add_topicprogress();
    topicProgress->set_topicname(_topicName);
    topicProgress->set_uint8filtermask(_uint8FilterMask);
    topicProgress->set_uint8maskresult(_uint8MaskResult);
    PartReaderProgress *partProgress = topicProgress->add_partprogress();
    partProgress->set_from(_from);
    partProgress->set_to(_to);
    partProgress->set_timestamp(timestamp);
    return progress;
}

void MockSwiftReader::init(
    const std::string topicName, uint16_t from, uint16_t to, int8_t uint8FilterMask, int8_t uint8MaskResult) {
    _topicName = topicName;
    _from = from;
    _to = to;
    _uint8FilterMask = uint8FilterMask;
    _uint8MaskResult = uint8MaskResult;
    _progress.push_back(createProgress(0));
}

} // namespace testlib
} // namespace swift
