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
#ifndef HIPPO_PROCESSSTARTWORKITEM_H
#define HIPPO_PROCESSSTARTWORKITEM_H

#include "util/Log.h"
#include "common/common.h"
#include "autil/WorkItem.h"
#include "hippo/DriverCommon.h"
#include "hippo/proto/Common.pb.h"
#include "sdk/default/CmdExecutor.h"

BEGIN_HIPPO_NAMESPACE(sdk);

enum ProcessError {
    ERROR_NONE = 0,
    ERROR_PREPARE_RESOURCE = 1,
    ERROR_START_CONTAINER = 2,
    ERROR_START_PROCESS = 3,
};

class ProcessStartWorkItem : public autil::WorkItem {
public:
    ProcessStartWorkItem();
    virtual ~ProcessStartWorkItem();
public:
    void process();
    static std::string getContainerName(const hippo::SlotId &slotId,
            const std::string& applicationId);

private:
    std::string getContainerParameter() const;
    std::string getUserDefineParameter() const;
    std::string getResourceParameter() const;
    std::string getMountParameter() const;
    std::string getImage() const;
    std::string getProcessCmd(const proto::ProcessInfo &processInfo,
                              const std::string& workDir) const;
    void joinProcessParameter(const std::string& key,
                              const std::string& value,
                              const std::string &sep,
                              std::string &str) const;
    void addSystemEnv(const std::string &workDir,
                      std::map<std::string, std::string> &envMap,
                      std::string &env) const;
    bool replaceByEnv(const std::string &oldValue,
                      const std::map<std::string, std::string> &envMap,
                      std::string &newValue) const;
    std::string getProcessWorkDir(const proto::ProcessInfo &processInfo) const;

private:
    const static std::string CUSTOM_CONTAINER_PARAMS;
    const static std::string CUSTOM_PROCESSOR_START_CMD;
    const static std::string CMD_LOCAL_MODE;
public:
    std::string applicationId;
    std::string role;
    std::string homeDir;
    hippo::SlotId slotId;
    proto::ProcessLaunchContext processContext;
    hippo::SlotResource slotResource;
    std::function<void(const hippo::SlotId &slotId,
        int64_t launchSignature, ProcessError ret, const std::string& errorMsg)> callback;
    CmdExecutor *cmdExecutor;
    int64_t launchSignature;
    bool _cmdLocalMode;
private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(ProcessStartWorkItem);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_PROCESSSTARTWORKITEM_H
