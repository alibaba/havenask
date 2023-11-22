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
#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatStreamCreator.h"

#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatServerManager.h"
#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatServerStream.h"

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatStreamCreator);

HeartbeatStreamCreator::HeartbeatStreamCreator() {
}

HeartbeatStreamCreator::~HeartbeatStreamCreator() {
}

std::string HeartbeatStreamCreator::methodName() const {
    return GIG_HEARTBEAT_METHOD_NAME;
}

GigServerStreamPtr HeartbeatStreamCreator::create() {
    auto manager = getManager();
    if (!manager) {
        return nullptr;
    }
    auto stream = std::make_shared<HeartbeatServerStream>(manager);
    manager->addStream(stream);
    return std::dynamic_pointer_cast<GigServerStream>(stream);
}

void HeartbeatStreamCreator::setManager(const HeartbeatServerManagerPtr &manager) {
    autil::ScopedLock scope(_managerMutex);
    _manager = manager;
}

HeartbeatServerManagerPtr HeartbeatStreamCreator::getManager() const {
    autil::ScopedLock scope(_managerMutex);
    return _manager;
}

} // namespace multi_call
