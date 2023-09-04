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

#include "autil/WorkItem.h"
#include "swift/admin/WorkerStatusWorkItem.h"
#include "swift/admin/modules/WorkerStatusModule.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace admin {

class BrokerStatusWorkItem : public autil::WorkItem {
public:
    BrokerStatusWorkItem(WorkerStatusModule *workerStatusModule, const protocol::BrokerInMetric &metric)
        : _workerStatusModule(workerStatusModule), _metric(metric) {}
    ~BrokerStatusWorkItem() {}

private:
    BrokerStatusWorkItem(const BrokerStatusWorkItem &);
    BrokerStatusWorkItem &operator=(const BrokerStatusWorkItem &);

public:
    virtual void process() { _workerStatusModule->updateBrokerInMetric(_metric); }

private:
    WorkerStatusModule *_workerStatusModule;
    protocol::BrokerInMetric _metric;
};

} // namespace admin
} // namespace swift
