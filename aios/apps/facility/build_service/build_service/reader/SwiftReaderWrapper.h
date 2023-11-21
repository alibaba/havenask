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
#pragma once

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "swift/client/SwiftReader.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace build_service { namespace reader {

class SwiftReaderWrapper
{
public:
    SwiftReaderWrapper(swift::client::SwiftReader* reader);
    ~SwiftReaderWrapper();

private:
    SwiftReaderWrapper(const SwiftReaderWrapper&);
    SwiftReaderWrapper& operator=(const SwiftReaderWrapper&);

public:
    bool init();
    swift::protocol::ErrorCode seekByTimestamp(int64_t timestamp, bool force = false);
    swift::protocol::ErrorCode seekByProgress(const swift::protocol::ReaderProgress& progress, bool force);
    void setTimestampLimit(int64_t timestampLimit, int64_t& acceptTimestamp);

    swift::protocol::ErrorCode read(int64_t timeout, int64_t& timeStamp, swift::protocol::Message& msg,
                                    swift::protocol::ReaderProgress& progress);
    swift::protocol::ErrorCode read(size_t maxMessageCount, int64_t timeout, int64_t& timeStamp,
                                    swift::protocol::Messages& msgs, swift::protocol::ReaderProgress& progress);
    swift::client::SwiftPartitionStatus getSwiftPartitionStatus() { return _reader->getSwiftPartitionStatus(); }

    swift::client::SwiftReader* getSwiftReader() const { return _reader; }

private:
    swift::protocol::ErrorCode getReaderProgress(swift::protocol::ReaderProgress& progress);

private:
    swift::client::SwiftReader* _reader;
    swift::protocol::Messages _swiftMessages;
    swift::protocol::ReaderProgress _lastProgress;
    int32_t _swiftCursor;
    int64_t _nextBufferTimestamp;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftReaderWrapper);

}} // namespace build_service::reader
