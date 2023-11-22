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

#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"
#include "build_service/worker/WorkerHeartbeat.h"
#include "worker_framework/ZkState.h"

namespace build_service { namespace worker {

class ZkWorkerHeartbeat : public WorkerHeartbeat
{
private:
    typedef worker_framework::ZkState ZkState;

public:
    ZkWorkerHeartbeat(proto::WorkerNodeBase* workerNode, const std::string& zkRoot, cm_basic::ZkWrapper* zkWrapper);
    ~ZkWorkerHeartbeat();

private:
    ZkWorkerHeartbeat(const ZkWorkerHeartbeat&);
    ZkWorkerHeartbeat& operator=(const ZkWorkerHeartbeat&);

public:
    bool start() override;
    void stop() override;

private:
    void workLoop();

private:
    std::string _zkRoot;
    cm_basic::ZkWrapper* _zkWrapper;
    ZkState* _currentState;
    ZkState* _targetState;
    autil::LoopThreadPtr _loopThreadPtr;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ZkWorkerHeartbeat);

}} // namespace build_service::worker
