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

#include <map>
#include <memory>

#include "autil/Lock.h"
#include "autil/LockFreeQueue.h"

using RawDocQueue = std::shared_ptr<autil::LockFreeQueue<std::string>>;
namespace suez {

class GlobalQueueManager {
public:
    GlobalQueueManager();
    ~GlobalQueueManager();

private:
    GlobalQueueManager(const GlobalQueueManager &);
    GlobalQueueManager &operator=(const GlobalQueueManager &);

public:
    static GlobalQueueManager *getInstance();
    RawDocQueue createQueue(const std::string &queueName);
    void releaseQueue(const std::string &queueName);

private:
    std::map<std::string, RawDocQueue> _docQueueMap;
    mutable autil::ThreadMutex _mutex;
};

} // namespace suez
