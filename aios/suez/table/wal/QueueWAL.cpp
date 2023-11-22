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
#include "suez/table/wal/QueueWAL.h"

#include "autil/LockFreeQueue.h"
#include "autil/Log.h"
#include "autil/result/Errors.h"
#include "build_service/common_define.h"
#include "suez/table/wal/WALConfig.h"

using namespace std;
using namespace autil;
using namespace autil::result;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, QueueWAL);

namespace suez {

const string QueueWAL::QUEUE_NAME = "queue_name";
const string QueueWAL::QUEUE_SIZE = "queue_size";

QueueWAL::QueueWAL() {}

QueueWAL::~QueueWAL() {}

bool QueueWAL::init(const WALConfig &config) {
    const auto &kvMap = config.sinkDescription;
    _queueName = build_service::getValueFromKeyValueMap(kvMap, QUEUE_NAME);
    if (_queueName.empty()) {
        AUTIL_LOG(ERROR, "queue name is empty for queue_wal.");
        return false;
    }
    _docQueuePtr = GlobalQueueManager::getInstance()->createQueue(_queueName);
    if (_docQueuePtr == nullptr) {
        AUTIL_LOG(ERROR, "get queue failed, queue_name[%s]", _queueName.c_str());
        return false;
    }
    string maxQueueSizeStr = build_service::getValueFromKeyValueMap(kvMap, QUEUE_SIZE);
    uint32_t maxQueueSize = 1000;
    if (!maxQueueSizeStr.empty() && StringUtil::fromString(maxQueueSizeStr, maxQueueSize)) {
        _maxQueueSize = maxQueueSize;
    }
    return true;
}

void QueueWAL::log(const vector<std::pair<uint16_t, std::string>> &strs, CallbackType done) {
    if (_docQueuePtr == nullptr) {
        done(RuntimeError::make("doc queue is null"));
        return;
    }
    if ((strs.size() + _docQueuePtr->Size()) >= _maxQueueSize) {
        done(RuntimeError::make("doc queue is full"));
        return;
    }
    vector<int64_t> timestamps;
    for (const auto &str : strs) {
        int64_t timestamp = TimeUtility::currentTime();
        RawDoc rawDoc = std::make_pair(timestamp, str.second);
        _docQueuePtr->Push(rawDoc);
        timestamps.push_back(timestamp);
    }
    done(std::move(timestamps));
}

void QueueWAL::stop() {
    _docQueuePtr.reset();
    GlobalQueueManager::getInstance()->releaseQueue(_queueName);
}

} // namespace suez
