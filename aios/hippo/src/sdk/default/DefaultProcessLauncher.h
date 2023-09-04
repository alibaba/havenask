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
#ifndef HIPPO_DEFAULTPROCESSLAUNCHER_H
#define HIPPO_DEFAULTPROCESSLAUNCHER_H

#include "util/Log.h"
#include "common/common.h"
#include "sdk/ProcessLauncherBase.h"
#include "sdk/default/ProcessController.h"
#include "hippo/proto/Slave.pb.h"

BEGIN_HIPPO_NAMESPACE(sdk);

class DefaultProcessLauncher : public ProcessLauncherBase
{
public:
    DefaultProcessLauncher();
    ~DefaultProcessLauncher();
private:
    DefaultProcessLauncher(const DefaultProcessLauncher &);
    DefaultProcessLauncher& operator=(const DefaultProcessLauncher &);
public:
    bool isPackageReady(const SlotInfo &slotInfo) const;

    bool isPreDeployPackageReady(const SlotInfo &slotInfo) const;

    bool isDataReady(const SlotInfo &slotInfo,
                     const std::vector<std::string> &dataNames) const;
    void launch(const std::map<std::string, std::set<hippo::SlotId> > &slotIds);
    bool resetSlot(const hippo::SlotId &slotId);
    void setLaunchMetas(const std::map<hippo::SlotId, LaunchMeta> &launchMetas);
    std::map<hippo::SlotId, LaunchMeta> getLaunchedMetas();
    void setSlotResources(std::map<hippo::SlotId, hippo::SlotResource> &slotResources)
    {
        _slotResources = slotResources;
    }
    virtual void setApplicationId(const std::string &appId);

private:
    void asyncLaunch(const std::map<std::string, std::set<hippo::SlotId> > &slotIds);
    void asyncLaunchOneRole(const std::string &role, const std::set<hippo::SlotId> &slotIds);
    void asyncLaunchOneSlot(const std::string& role,
                            const hippo::SlotId &slotId,
                            const hippo::ProcessContext &context);
    bool getLaunchSignature(const hippo::SlotId &slotId,
                            int64_t &signature);
    bool needLaunch(const hippo::SlotId &slotId);
    void getReleasedSlots(
            const std::map<std::string, std::set<hippo::SlotId> > &slotIds,
            std::map<std::string, std::set<hippo::SlotId> > &releasedSlots);
    void stopProcess(const std::map<std::string, std::set<hippo::SlotId> > &releasedSlots);
    void clearLaunchedSignature(const hippo::SlotId &slotId);
    void setLaunchedSignature(const hippo::SlotId &slotId, int64_t launchSignature);
    void callback(const hippo::SlotId &slotId,
                  int64_t launchSignature,
                  ProcessError ret,
                  const std::string& errorMsg);
    bool getSlotResource(const hippo::SlotId &slotId, hippo::SlotResource &slotResource);
private:
    std::string _homeDir;
    ProcessController _controller;
    std::map<hippo::SlotId, hippo::SlotResource> _slotResources;
    std::map<std::string, std::set<hippo::SlotId> > _role2SlotIds;
    autil::TerminateNotifier _counter;
    //slotId to signature and timestamp
    std::map<hippo::SlotId, std::pair<int64_t, int64_t> > _launchedMetas;
    int64_t _processCheckInterval; //us
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(DefaultProcessLauncher);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_DEFAULTPROCESSLAUNCHER_H
