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
#include "build_service/worker/ZkWorkerHeartbeat.h"

#include "build_service/common/PathDefine.h"
#include "fslib/util/FileUtil.h"
#include "worker_framework/ZkState.h"

using namespace std;
using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::util;
using namespace autil;

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, ZkWorkerHeartbeat);

ZkWorkerHeartbeat::ZkWorkerHeartbeat(WorkerNodeBase* workerNode, const string& zkRoot, cm_basic::ZkWrapper* zkWrapper)
    : WorkerHeartbeat(workerNode)
    , _zkRoot(zkRoot)
    , _zkWrapper(zkWrapper)
    , _currentState(NULL)
    , _targetState(NULL)
{
}

ZkWorkerHeartbeat::~ZkWorkerHeartbeat()
{
    stop();
    DELETE_AND_SET_NULL(_targetState);
    DELETE_AND_SET_NULL(_currentState);
}

bool ZkWorkerHeartbeat::start()
{
    string currentStateFile = fslib::util::FileUtil::joinFilePath(_zkRoot, "current");
    string targetStateFile = fslib::util::FileUtil::joinFilePath(_zkRoot, "target");
    _currentState = new ZkState(_zkWrapper, currentStateFile);
    _targetState = new ZkState(_zkWrapper, targetStateFile);

    _loopThreadPtr = LoopThread::createLoopThread(bind(&ZkWorkerHeartbeat::workLoop, this), 1 * 1000 * 1000); // 1s
    if (_loopThreadPtr.get() == NULL) {
        DELETE_AND_SET_NULL(_currentState);
        DELETE_AND_SET_NULL(_targetState);
        return false;
    }
    return true;
}

void ZkWorkerHeartbeat::stop() { _loopThreadPtr.reset(); }

void ZkWorkerHeartbeat::workLoop()
{
    string targetStatus;
    if (_targetState->read(targetStatus) != ZkState::EC_FAIL) {
        _workerNode->setTargetStatusStr(targetStatus);
    }
    string currentStatus = _workerNode->getCurrentStatusStr();
    _currentState->write(currentStatus);
}

}} // namespace build_service::worker
