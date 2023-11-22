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

#include <functional>
#include <string>
#include <utility>

#include "aios/autil/autil/ThreadPool.h"
#include "autil/LoopThread.h"
#include "autil/WorkItem.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/worker/WorkerHeartbeat.h"
#include "build_service/worker/WorkerStateHandler.h"

namespace build_service { namespace worker {

class WorkerHeartbeatExecutor
{
public:
    WorkerHeartbeatExecutor(proto::WorkerNodeBase* workerNode, WorkerStateHandler* handler, WorkerHeartbeat* heartbeat);
    virtual ~WorkerHeartbeatExecutor();

private:
    WorkerHeartbeatExecutor(const WorkerHeartbeatExecutor&);
    WorkerHeartbeatExecutor& operator=(const WorkerHeartbeatExecutor&);

public:
    virtual bool start();
    void setExitCallback(std::function<void()>&& exitFunction) { _exitCallBackFunc = std::move(exitFunction); }

private:
    class WorkItemImpl : public autil::WorkItem
    {
    public:
        WorkItemImpl(WorkerStateHandler* handler, const std::string& target, const std::string& targetResources)
            : _handler(handler)
            , _target(target)
            , _targetResources(targetResources)
        {
        }
        void process() override { _handler->handleTargetState(_target, _targetResources); }

    private:
        WorkerStateHandler* _handler;
        std::string _target;
        std::string _targetResources;
    };
    BS_TYPEDEF_PTR(WorkItemImpl);
    // virtual for test
    virtual void workLoop();
    virtual void exit(const std::string& message);

private:
    proto::WorkerNodeBase* _workerNode;
    WorkerStateHandler* _handler;
    WorkerHeartbeat* _heartbeat;
    std::function<void()> _exitCallBackFunc;

protected:
    autil::LoopThreadPtr _loopThreadPtr;

private:
    // autil::ThreadPtr _workingThreadPtr;
    autil::ThreadPool _threadPool;
    bool _hasHandleTarget;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(WorkerHeartbeatExecutor);

}} // namespace build_service::worker
