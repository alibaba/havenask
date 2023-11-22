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

#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace worker {

class WorkerHeartbeat
{
public:
    WorkerHeartbeat(proto::WorkerNodeBase* workerNode);
    virtual ~WorkerHeartbeat();

private:
    WorkerHeartbeat(const WorkerHeartbeat&);
    WorkerHeartbeat& operator=(const WorkerHeartbeat&);

public:
    virtual bool start() = 0;
    virtual void stop() = 0;

protected:
    proto::WorkerNodeBase* _workerNode;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(WorkerHeartbeat);

}} // namespace build_service::worker
