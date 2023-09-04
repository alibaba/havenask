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
#include "build_service/worker/WorkerHeartbeatExecutor.h"

#include "build_service/proto/HeartbeatDefine.h"
#include "build_service/worker/ZkWorkerHeartbeat.h"

using namespace std;
using namespace autil;

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, WorkerHeartbeatExecutor);

WorkerHeartbeatExecutor::WorkerHeartbeatExecutor(proto::WorkerNodeBase* workerNode, WorkerStateHandler* handler,
                                                 WorkerHeartbeat* heartbeat)
    : _workerNode(workerNode)
    , _handler(handler)
    , _heartbeat(heartbeat)
    , _threadPool(1 /*thread num*/, 1 /*queue size*/, true /*stop if has exception*/)
    , _hasHandleTarget(false)
{
}

WorkerHeartbeatExecutor::~WorkerHeartbeatExecutor()
{
    _loopThreadPtr.reset();
    _threadPool.stop(ThreadPool::STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION);
    DELETE_AND_SET_NULL(_heartbeat);
    DELETE_AND_SET_NULL(_handler);
    DELETE_AND_SET_NULL(_workerNode);
}

bool WorkerHeartbeatExecutor::start()
{
    if (!_heartbeat) {
        return false;
    }
    if (dynamic_cast<ZkWorkerHeartbeat*>(_heartbeat) != NULL) {
        if (!_heartbeat->start()) {
            return false;
        }
    }
    if (!_threadPool.start("bs_working_thread")) {
        return false;
    }
    _loopThreadPtr = LoopThread::createLoopThread(bind(&WorkerHeartbeatExecutor::workLoop, this),
                                                  proto::WORKER_EXEC_HEARTBEAT_INTERVAL);
    return _loopThreadPtr.get() != NULL;
}

void WorkerHeartbeatExecutor::workLoop()
{
    try {
        _threadPool.checkException();
    } catch (...) {
        exit("bs working thread has exception, prepare to exit");
    }
    _handler->reportMetrics(_hasHandleTarget);

    string target, requiredHost;
    // TODO(xiaohao.yxh) maybe check host here?
    _workerNode->getTargetStatusStr(target, requiredHost);
    string targetResources, usingResources;
    _workerNode->fillTargetDependResources(targetResources);
    if (_handler->needRestartProcess(target)) {
        exit("worker need restart, prepare to exit");
    }

    bool needSuspend = _handler->needSuspendTask(target);
    // in case worker has been suspended and receive resume target
    if (_handler->isSuspended() && !needSuspend) {
        exit("suspended worker received non-suspend target, prepare to exit");
    }

    if (needSuspend) {
        if (_hasHandleTarget) {
            exit("suspend command received while worker"
                 " is processing, prepare to exit");
        } else {
            _handler->setSuspendStatus(true);
        }
    }
    string current;
    if (_handler->isProcessing()) {
        _handler->getCurrentState(current);
        _workerNode->setCurrentStatusStr(current, "");
        return;
    }

    _handler->getCurrentState(current);
    _workerNode->setCurrentStatusStr(current, "");
    _handler->getUsingResources(usingResources);
    _workerNode->setCurrentResources(usingResources);

    if (_handler->isSuspended()) {
        return;
    }

    if (!target.empty()) { // if target received
        WorkItemImpl* newWorkItem = new WorkItemImpl(_handler, target, targetResources);
        auto errorType = _threadPool.pushWorkItem(newWorkItem);
        if (errorType != ThreadPool::ERROR_NONE) {
            newWorkItem->destroy();
            exit("handle target in thread pool failed, restart");
        }
        _hasHandleTarget = true;
    }

    if (_handler->hasFatalError()) {
        string message = "fatal error happened, prepare to exit";
        exit(message);
    }
}

void WorkerHeartbeatExecutor::exit(const string& message)
{
    BS_LOG(ERROR, "%s", message.c_str());
    cerr << message << endl;
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, message);
    usleep(proto::ADMIN_HEARTBEAT_INTERVAL * 2 + proto::WORKER_EXEC_HEARTBEAT_INTERVAL * 2);
    // wait current collected by admin, maybe not accurate

    if (_exitCallBackFunc) {
        BS_LOG(ERROR, "run ExitCallBack function before execute exit().");
        _exitCallBackFunc();
        _exitCallBackFunc = nullptr;
    }

    BS_LOG_FLUSH();
    BEEPER_CLOSE();
    BS_LOG(ERROR, "exit");
    _exit(-1);
}

}} // namespace build_service::worker
