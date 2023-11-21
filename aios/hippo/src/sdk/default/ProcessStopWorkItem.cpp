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
#include "sdk/default/ProcessStopWorkItem.h"
#include "util/PathUtil.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
USE_HIPPO_NAMESPACE(util);

BEGIN_HIPPO_NAMESPACE(sdk);

const string ProcessStopWorkItem::PROCESS_DELAY_KILL_IN_US = "PROCESS_DELAY_KILL_IN_US";

HIPPO_LOG_SETUP(sdk, ProcessStopWorkItem);

ProcessStopWorkItem::ProcessStopWorkItem()
    : cmdExecutor(nullptr)
{
}

ProcessStopWorkItem::~ProcessStopWorkItem() {
}

void ProcessStopWorkItem::process() {
    if (!cmdExecutor) {
        HIPPO_LOG(ERROR, "cmdExecutor is null");
        return;
    }
    string container = ProcessStopWorkItem::getContainerName(slotId, applicationId);
    int64_t delayKillTime = autil::EnvUtil::getEnv<int64_t>(PROCESS_DELAY_KILL_IN_US, 15*1000L*1000L);
    usleep(delayKillTime);
    HIPPO_LOG(DEBUG, "stopProcess, processNames.size[%ld]", processNames.size());
    for (auto &processName : processNames) {
        HIPPO_LOG(DEBUG, "stopProcess, [%s]", processName.c_str());
        vector<int32_t> pids;
        int32_t pid =-1;
        string workDirPrefix = "/";
        if (cmdExecutor->checkProcessExist(slotId.slaveAddress,
                        container, processName, workDirPrefix, pid))
        {
            pids.push_back(pid);
        }
        if (pids.size() > 0) {
            for (auto pid : pids) {
                cmdExecutor->stopProcess(slotId.slaveAddress, container, pid);
            }
            int32_t count = 20;
            while(count--) {
                if (!cmdExecutor->checkProcessExist(slotId.slaveAddress,
                                container, processName, workDirPrefix, pid))
                {
                    break;
                }
                usleep(1000000);
            }
        }
    }
    cmdExecutor->stopContainer(slotId.slaveAddress, container);
}

string ProcessStopWorkItem::getContainerName(const hippo::SlotId& slotId,
        const string& applicationId)
{
    const static string DOCKER_PREFIX = "havenask_container_" + applicationId + "_";
    return DOCKER_PREFIX + StringUtil::toString(slotId.id);
}

END_HIPPO_NAMESPACE(sdk);
