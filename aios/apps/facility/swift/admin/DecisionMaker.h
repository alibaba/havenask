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

#include <stdint.h>

#include "autil/Log.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/common/Common.h"

namespace swift {
namespace admin {

class DecisionMaker {
public:
    DecisionMaker(){};
    virtual ~DecisionMaker(){};
    DecisionMaker(const DecisionMaker &) = delete;
    DecisionMaker &operator=(const DecisionMaker &) = delete;

public:
    virtual void makeDecision(TopicMap &topicMap, WorkerMap &aliveBroker) = 0;

protected:
    virtual bool assign2Worker(TopicInfoPtr &tinfo, uint32_t partId, WorkerInfoPtr &targetWorker) const = 0;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(DecisionMaker);

} // namespace admin
} // namespace swift
