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
#include "sdk/default/ProcessController.h"

using namespace std;
using namespace autil;
BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, ProcessController);

ProcessController::ProcessController()
    : _threadPool(nullptr)
{
}

ProcessController::~ProcessController() {
    stop();
    DELETE_AND_SET_NULL_HIPPO(_threadPool);
}

bool ProcessController::started() const {
    return _threadPool != nullptr;
}

bool ProcessController::start() {
    if (!_threadPool) {
        _threadPool = new ThreadPool(4, 5000);
    }
    return _threadPool->start("PorcessControll");
}

void ProcessController::stop() {
    if (_threadPool) {
        _threadPool->stop();
    }
}

bool ProcessController::startProcess(ProcessStartWorkItem *workItem) {
    return _threadPool->pushWorkItem(workItem, false) == ThreadPool::ERROR_NONE;
}

void ProcessController::stopProcess(const hippo::SlotId &slotId) {
    string container = ProcessStartWorkItem::getContainerName(slotId, _applicationId);
    _cmdExecutor.stopContainer(slotId.slaveAddress, container);
}

bool ProcessController::resetSlot(const hippo::SlotId &slotId) {
    string container = ProcessStartWorkItem::getContainerName(slotId, _applicationId);
    _cmdExecutor.stopContainer(slotId.slaveAddress, container);
    return true;
}


END_HIPPO_NAMESPACE(sdk);
