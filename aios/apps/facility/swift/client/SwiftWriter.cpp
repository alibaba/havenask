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
#include "swift/client/SwiftWriter.h"

#include <cstddef>
#include <unistd.h>

#include "autil/TimeUtility.h"
#include "swift/common/MessageInfo.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftWriter);

SwiftWriter::SwiftWriter()
    : _forceSend(false)
    , _waitLoopInterval(10 * 1000) // 10ms
{}

SwiftWriter::~SwiftWriter() {}

ErrorCode SwiftWriter::writes(vector<MessageInfo> &msgInfos, bool waitSend) {
    ErrorCode ec = ERROR_NONE;
    for (size_t i = 0; i < msgInfos.size(); i++) {
        ec = write(msgInfos[i]);
        if (ec != protocol::ERROR_NONE) {
            return ec;
        }
    }
    if (waitSend) {
        ec = waitSent();
    }
    return ec;
}

void SwiftWriter::setForceSend(bool forceSend) { _forceSend = forceSend; }

ErrorCode SwiftWriter::waitFinished(int64_t timeout) {
    auto func = std::bind(&SwiftWriter::isFinished, this);
    return wait(func, timeout);
}

ErrorCode SwiftWriter::waitSent(int64_t timeout) { return wait(std::bind(&SwiftWriter::isAllSent, this), timeout); }

ErrorCode SwiftWriter::wait(const std::function<bool()> &func, int64_t timeout) {
    ScopedLock lock(_waitMutex);
    int64_t leftTime = timeout;
    int64_t endTime = TimeUtility::currentTime() + leftTime;
    setForceSend(true);
    do {
        leftTime = endTime - TimeUtility::currentTime();
        if (leftTime < (int64_t)_waitLoopInterval) {
            break;
        }
        usleep(_waitLoopInterval);
        leftTime = endTime - TimeUtility::currentTime();
    } while (leftTime > 0 && !func());

    ErrorCode ec = ERROR_NONE;
    if (!func()) {
        AUTIL_LOG(WARN, "not all message send, checkpointId[%ld]", getCommittedCheckpointId());
        ec = ERROR_CLIENT_SOME_MESSAGE_SEND_FAILED;
    }
    setForceSend(false);
    return ec;
}

} // namespace client
} // namespace swift
