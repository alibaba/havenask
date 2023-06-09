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
#include "build_service/reader/SwiftReaderWrapper.h"

using namespace std;

using namespace swift::protocol;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, SwiftReaderWrapper);

namespace {
void deepCopyReaderProgress(const swift::protocol::ReaderProgress& input, swift::protocol::ReaderProgress& output)
{
    output.Clear();
    for (size_t i = 0; i < input.progress_size(); i++) {
        auto singleProgress = output.add_progress();
        singleProgress->CopyFrom(input.progress(i));
    }
}
} // namespace

SwiftReaderWrapper::SwiftReaderWrapper(swift::client::SwiftReader* reader)
    : _reader(reader)
    , _swiftCursor(0)
    , _nextBufferTimestamp(0)
{
}

SwiftReaderWrapper::~SwiftReaderWrapper()
{
    if (_reader) {
        delete _reader;
    }
}

bool SwiftReaderWrapper::init()
{
    auto ret = _reader->getReaderProgress(_lastProgress);
    if (ret != swift::protocol::ERROR_NONE) {
        BS_LOG(ERROR, "get last progress failed");
        return false;
    }
    return true;
}

ErrorCode SwiftReaderWrapper::seekByTimestamp(int64_t timestamp, bool force)
{
    auto ec = _reader->seekByTimestamp(timestamp, force);
    if (ec != swift::protocol::ERROR_NONE) {
        BS_LOG(ERROR, "seek by timestamp failed");
        return ec;
    }
    return getReaderProgress(_lastProgress);
}

ErrorCode SwiftReaderWrapper::seekByProgress(const swift::protocol::ReaderProgress& progress, bool force)
{
    auto ec = _reader->seekByProgress(progress, force);
    if (ec != swift::protocol::ERROR_NONE) {
        BS_LOG(ERROR, "seek by progress failed");
        return ec;
    }
    return getReaderProgress(_lastProgress);
}

void SwiftReaderWrapper::setTimestampLimit(int64_t timestampLimit, int64_t& acceptTimestamp)
{
    _reader->setTimestampLimit(timestampLimit, acceptTimestamp);
}

ErrorCode SwiftReaderWrapper::read(int64_t timeout, int64_t& timeStamp, Message& msg,
                                   swift::protocol::ReaderProgress& progress)
{
    if (_swiftCursor < _swiftMessages.msgs_size()) {
        _swiftMessages.mutable_msgs(_swiftCursor)->Swap(&msg);
        _swiftCursor++;
        if (_swiftCursor >= _swiftMessages.msgs_size()) {
            timeStamp = _nextBufferTimestamp;
        } else {
            timeStamp = min(_nextBufferTimestamp, _swiftMessages.msgs(_swiftCursor).timestamp());
        }
        return getReaderProgress(progress);
    } else {
        auto ret = _reader->read(timeStamp, msg, timeout);
        if (ret != swift::protocol::ERROR_NONE) {
            return ret;
        }
        return getReaderProgress(progress);
    }
}

ErrorCode SwiftReaderWrapper::read(size_t maxMessageCount, int64_t timeout, int64_t& timeStamp, Messages& msgs,
                                   swift::protocol::ReaderProgress& progress)
{
    if (_swiftCursor >= _swiftMessages.msgs_size()) {
        _swiftMessages.clear_msgs();
        auto ret = _reader->read(_nextBufferTimestamp, _swiftMessages, timeout);
        if (ret != swift::protocol::ERROR_NONE) {
            timeStamp = _nextBufferTimestamp;
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
    return getReaderProgress(progress);
}

swift::protocol::ErrorCode SwiftReaderWrapper::getReaderProgress(swift::protocol::ReaderProgress& progress)
{
    if (_swiftCursor < _swiftMessages.msgs_size()) {
        deepCopyReaderProgress(_lastProgress, progress);
        return swift::protocol::ERROR_NONE;
    }

    swift::protocol::ReaderProgress newProgress;
    // getReaderProgress will apped new progress from the end
    auto ret = _reader->getReaderProgress(newProgress);
    if (ret != swift::protocol::ERROR_NONE) {
        return ret;
    }
    deepCopyReaderProgress(newProgress, _lastProgress);
    deepCopyReaderProgress(newProgress, progress);
    return swift::protocol::ERROR_NONE;
}

}} // namespace build_service::reader
