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
#ifndef HIPPO_PROCESSSTOPWORKITEM_H
#define HIPPO_PROCESSSTOPWORKITEM_H

#include "util/Log.h"
#include "common/common.h"
#include "autil/WorkItem.h"
#include "hippo/DriverCommon.h"
#include "hippo/proto/Common.pb.h"
#include "sdk/default/CmdExecutor.h"

BEGIN_HIPPO_NAMESPACE(sdk);

class ProcessStopWorkItem : public autil::WorkItem {
public:
    ProcessStopWorkItem();
    virtual ~ProcessStopWorkItem();
public:
    void process();
    static std::string getContainerName(const hippo::SlotId &slotId,
            const std::string& applicationId);
public:
    CmdExecutor *cmdExecutor;
    std::string applicationId;
    hippo::SlotId slotId;
    std::vector<std::string> processNames;
private:
    const static std::string PROCESS_DELAY_KILL_IN_US;
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(ProcessStopWorkItem);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_PROCESSSTOPWORKITEM_H
