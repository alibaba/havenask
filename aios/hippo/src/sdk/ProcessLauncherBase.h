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
#ifndef HIPPO_PROCESSLAUNCHERBASE_H
#define HIPPO_PROCESSLAUNCHERBASE_H

#include "util/Log.h"
#include "common/common.h"
#include "hippo/DriverCommon.h"
#include "hippo/proto/Common.pb.h"
#include "autil/Lock.h"

BEGIN_HIPPO_NAMESPACE(sdk);

typedef std::map<std::string, hippo::ProcessContext> ScopeProcessContextMap;

struct RoleProcessContext {
    ScopeProcessContextMap basicContext;
    std::map<hippo::SlotId, ScopeProcessContextMap> customContext;
};
typedef std::map<std::string, RoleProcessContext> RoleProcessContextMap;

class ProcessLauncherBase
{
public:
    ProcessLauncherBase();
    virtual ~ProcessLauncherBase();
private:
    ProcessLauncherBase(const ProcessLauncherBase &);
    ProcessLauncherBase& operator=(const ProcessLauncherBase &);

public:
    void setRoleProcessContext(const std::string &role,
                               const hippo::ProcessContext &context,
                               const std::string &scope = "");

    void setSlotProcessContext(const std::vector<SlotInfo> &slotVec,
                               const hippo::ProcessContext &context,
                               const std::string &scope = "");

    void setSlotProcessContext(const std::vector<std::pair<std::vector<SlotInfo>, hippo::ProcessContext> > &slotLaunchPlans,
                               const std::string &scope = "");

    void clearRoleProcessContext(const std::string &role);

    bool restartProcess(const SlotInfo &slotInfo,
                        const std::vector<std::string> &processNames);

    bool isProcessReady(const SlotInfo &slotInfo,
                        const std::vector<std::string> &processNames) const;

    virtual bool updatePackages(const SlotInfo &slotInfo,
                                const std::vector<hippo::PackageInfo> &packages);

    virtual bool updatePreDeployPackages(const SlotInfo &slotInfo,
                                 const std::vector<hippo::PackageInfo> &packages);
    virtual bool updateDatas(const SlotInfo &slotInfo,
                             const std::vector<hippo::DataInfo> &datas,
                             bool force);
    virtual bool isPackageReady(const SlotInfo &slotInfo) const = 0;

    virtual bool isPreDeployPackageReady(const SlotInfo &slotInfo) const = 0;

    virtual bool isDataReady(const SlotInfo &slotInfo,
                     const std::vector<std::string> &dataNames) const = 0;

    virtual void launch(const std::map<std::string, std::set<hippo::SlotId> > &slotIds) = 0;
    virtual bool resetSlot(const hippo::SlotId &slotId) = 0;

    virtual void setLaunchMetas(const std::map<hippo::SlotId, LaunchMeta> &launchMetas);
    // public for test
    bool getSlotProcessContext(const std::string &role,
                               const hippo::SlotId &slotId,
                               hippo::ProcessContext *context) const;

    virtual void setApplicationId(const std::string &appId);

    void setAppChecksum(int64_t checksum);

    void setProcInstanceIds(
            const hippo::SlotId &slotId, hippo::ProcessContext *context) const;

protected:
    void clearUselessContexts(
            const std::map<std::string, std::set<hippo::SlotId> > &slotIds);
    void doSetSlotProcessContext(const std::string &role,
                                 const hippo::SlotId &slotId,
                                 const hippo::ProcessContext &context,
                                 const std::string &scope = "");
    bool doGetSlotProcessContext(const std::string &role,
                                 const hippo::SlotId &slotId,
                                 hippo::ProcessContext *context,
                                 const std::string &scope = "") const;

protected:
    bool getProcessContext(const ScopeProcessContextMap &contextMap,
                           const std::string &scope,
                           hippo::ProcessContext *context) const;
    hippo::ProcessInfo* getProcessInfo(hippo::ProcessContext &processContext,
            const std::string &procName) const;
    int64_t getProcInstanceId(const hippo::SlotId &slotId,
                              const std::string &procName) const;
    bool doMergeSlotProcessContext(const std::string &role,
                                   const hippo::SlotId &slotId,
                                   hippo::ProcessContext *context) const;
    void doMergeProcessContext(const ScopeProcessContextMap &slotContextMap,
                               const ScopeProcessContextMap &roleContextMap,
                               hippo::ProcessContext *context) const;
    void mergeScopeProcessContext(const ScopeProcessContextMap &slotContextMap,
                                  hippo::ProcessContext *context) const;

protected:
    mutable autil::ThreadMutex _mutex;
    std::string _applicationId;
    int64_t _appChecksum;
    RoleProcessContextMap _roleProcessContextMap;
    std::map<hippo::SlotId, LaunchMeta> _launchMetas;
    std::map<hippo::SlotId, std::map<std::string, int64_t> > _procInstanceIds;
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(ProcessLauncherBase);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_PROCESSLAUNCHERBASE_H
