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

#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "swift/admin/modules/BaseModule.h"

namespace swift {
namespace protocol {
class BrokerInMetric;
class BrokerStatusRequest;
class BrokerStatusResponse;
} // namespace protocol

namespace admin {
class TopicInStatusManager;
class WorkerTable;

class WorkerStatusModule : public BaseModule {
public:
    WorkerStatusModule();
    ~WorkerStatusModule();
    WorkerStatusModule(const WorkerStatusModule &) = delete;
    WorkerStatusModule &operator=(const WorkerStatusModule &) = delete;

public:
    bool doInit() override;
    bool doLoad() override;
    bool doUnload() override;

public:
    bool reportBrokerStatus(const protocol::BrokerStatusRequest *request, protocol::BrokerStatusResponse *response);
    bool updateBrokerInMetric(const protocol::BrokerInMetric &bMetric);

private:
    autil::ThreadPoolPtr _threadPool;
    WorkerTable *_workerTable = nullptr;
    TopicInStatusManager *_topicInStatusManager = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace admin
} // namespace swift
