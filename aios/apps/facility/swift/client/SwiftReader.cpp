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
#include "swift/client/SwiftReader.h"

#include <iosfwd>

#include "autil/TimeUtility.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace autil;
using namespace swift::protocol;

namespace swift {
namespace client {

SwiftReader::SwiftReader() {}
SwiftReader::~SwiftReader() {}

ErrorCode SwiftReader::read(protocol::Message &msg, int64_t timeout) {
    int64_t timestamp = 0;
    return read(timestamp, msg, timeout);
}

ErrorCode SwiftReader::read(protocol::Messages &msgs, int64_t timeout) {
    int64_t timestamp = 0;
    return read(timestamp, msgs, timeout);
}

ErrorCode SwiftReader::read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout) {
    protocol::Message msg;
    ErrorCode ec = read(timeStamp, msg, timeout);
    if (ec == ERROR_NONE) {
        *msgs.add_msgs() = msg;
    }
    return ec;
}

} // namespace client
} // namespace swift
