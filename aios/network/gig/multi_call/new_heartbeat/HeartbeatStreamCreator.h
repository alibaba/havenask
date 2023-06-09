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

#include "aios/network/gig/multi_call/stream/GigServerStreamCreator.h"
#include "autil/Lock.h"

namespace multi_call {

class HeartbeatServerManager;

class HeartbeatStreamCreator : public GigServerStreamCreator
{
public:
    HeartbeatStreamCreator();
    ~HeartbeatStreamCreator();
private:
    HeartbeatStreamCreator(const HeartbeatStreamCreator &);
    HeartbeatStreamCreator &operator=(const HeartbeatStreamCreator &);
public:
    std::string methodName() const override;
    GigServerStreamPtr create() override;
public:
    void setManager(const std::shared_ptr<HeartbeatServerManager> &manager);
private:
    std::shared_ptr<HeartbeatServerManager> getManager() const;
private:
    mutable autil::ThreadMutex _managerMutex;
    std::shared_ptr<HeartbeatServerManager> _manager;
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HeartbeatStreamCreator);

}
