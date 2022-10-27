#include "build_service/reader/SwiftReaderWrapper.h"

using namespace std;

SWIFT_USE_NAMESPACE(protocol);

namespace build_service {
namespace reader {
BS_LOG_SETUP(reader, SwiftReaderWrapper);

SwiftReaderWrapper::SwiftReaderWrapper(SWIFT_NS(client)::SwiftReader *reader)
    : _reader(reader), _swiftCursor(0), _nextBufferTimestamp(0) {}

SwiftReaderWrapper::~SwiftReaderWrapper() {
    if (_reader) {
        delete _reader;
    }
}

ErrorCode SwiftReaderWrapper::seekByTimestamp(int64_t timestamp, bool force) {
    return _reader->seekByTimestamp(timestamp, force);
}

void SwiftReaderWrapper::setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) {
    _reader->setTimestampLimit(timestampLimit, acceptTimestamp);
}

ErrorCode SwiftReaderWrapper::read(int64_t &timeStamp, Message &msg,
                                   int64_t timeout) {
    if (_swiftCursor < _swiftMessages.msgs_size()) {
        _swiftMessages.mutable_msgs(_swiftCursor)->Swap(&msg);
        _swiftCursor++;
        if (_swiftCursor >= _swiftMessages.msgs_size()) {
            timeStamp = _nextBufferTimestamp;
        } else {
            timeStamp =
                min(_nextBufferTimestamp, _swiftMessages.msgs(_swiftCursor).timestamp());
        }
        return SWIFT_NS(protocol)::ERROR_NONE;
    } else {
        return _reader->read(timeStamp, msg, timeout);
    }
}

ErrorCode SwiftReaderWrapper::read(int64_t &timeStamp, Messages &msgs, size_t maxMessageCount,
                                   int64_t timeout) {
    if (_swiftCursor >= _swiftMessages.msgs_size()) {
        _swiftMessages.clear_msgs();
        auto ret = _reader->read(_nextBufferTimestamp, _swiftMessages, timeout);
        if (ret != SWIFT_NS(protocol)::ERROR_NONE) {
            return ret;
        }
        _swiftCursor = 0;
    }

    msgs.clear_msgs();
    int32_t msgEnd = _swiftMessages.msgs_size();

    for (size_t i = 0; i < maxMessageCount && _swiftCursor < msgEnd; ++i, ++_swiftCursor) {
        msgs.add_msgs()->Swap(_swiftMessages.mutable_msgs(_swiftCursor));
    }
    timeStamp = _nextBufferTimestamp;
    if (_swiftCursor < msgEnd) {
        timeStamp = min(timeStamp, _swiftMessages.msgs(_swiftCursor).timestamp());
    }
    return ERROR_NONE;
}
}
}
