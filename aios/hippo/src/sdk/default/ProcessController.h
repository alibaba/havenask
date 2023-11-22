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
#ifndef HIPPO_PROCESSCONTROLLER_H
#define HIPPO_PROCESSCONTROLLER_H

#include "util/Log.h"
#include "common/common.h"
#include "hippo/DriverCommon.h"
#include "autil/ThreadPool.h"
#include "sdk/default/ProcessStartWorkItem.h"
BEGIN_HIPPO_NAMESPACE(sdk);

class ProcessController
{
public:
    ProcessController();
    ~ProcessController();
private:
    ProcessController(const ProcessController &);
    ProcessController& operator=(const ProcessController &);
public:
    bool started() const;
    bool start();
    void stop();
public:
    bool startProcess(ProcessStartWorkItem *workItem);
    void stopProcess(const hippo::SlotId &slotId, std::vector<std::string> &processNames);
    bool resetSlot(const hippo::SlotId &slotId);
    CmdExecutor *getCmdExecutor() { return &_cmdExecutor; }
    void setApplicationId(const std::string &appId) {
        _applicationId = appId;
    }
    bool checkProcessExist(const ProcessStartWorkItem *workItem);

private:
    std::string getContainerName(const hippo::SlotId& slotId) const;
private:
    std::string _applicationId;
    autil::ThreadPool *_threadPool;
    CmdExecutor _cmdExecutor;
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(ProcessController);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_PROCESSCONTROLLER_H
