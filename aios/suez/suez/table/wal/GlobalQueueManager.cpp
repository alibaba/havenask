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
#include "suez/table/wal/GlobalQueueManager.h"

#include <autil/Log.h>

#include "autil/LockFreeQueue.h"

using namespace std;
using namespace autil;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, GlobalQueueManager);

namespace suez {

GlobalQueueManager queueManager;

GlobalQueueManager::GlobalQueueManager() {}

GlobalQueueManager::~GlobalQueueManager() {}

RawDocQueue GlobalQueueManager::createQueue(const std::string &queueName) {
    ScopedLock lock(_mutex);
    auto it = _docQueueMap.find(queueName);
    if (it != _docQueueMap.end()) {
        return it->second;
    }
    RawDocQueue docQueuePtr(new LockFreeQueue<std::string>());
    _docQueueMap[queueName] = docQueuePtr;
    return docQueuePtr;
}

void GlobalQueueManager::releaseQueue(const std::string &queueName) {
    ScopedLock lock(_mutex);
    auto it = _docQueueMap.find(queueName);
    if (it != _docQueueMap.end()) {
        _docQueueMap.erase(it);
    }
}

GlobalQueueManager *GlobalQueueManager::getInstance() { return &queueManager; }

} // namespace suez
