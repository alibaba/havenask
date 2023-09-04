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
#ifndef CARBON_CARBONDRIVERWRAPPERIMPL_H
#define CARBON_CARBONDRIVERWRAPPERIMPL_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/CarbonDriverWrapper.h"
#include "master/CarbonDriver.h"
#include "master/CarbonDriverCache.h"
#include "master/SlotWorkerNodeManager.h"
#include "service/CarbonMasterServiceWrapper.h"
#include "service/CarbonOpsServiceImpl.h"
#include "monitor/CarbonMonitor.h"

BEGIN_CARBON_NAMESPACE(master);

class CarbonDriverWrapperImpl : public CarbonDriverWrapper
{
public:
    CarbonDriverWrapperImpl();
    virtual ~CarbonDriverWrapperImpl();
private:
    CarbonDriverWrapperImpl(const CarbonDriverWrapperImpl &);
    CarbonDriverWrapperImpl& operator=(const CarbonDriverWrapperImpl &);
public:
    bool init(const std::string &applicationId,
              const std::string &hippoZk,
              const std::string &carbonZk,
              const std::string &backupRoot,
              worker_framework::LeaderElector *leaderElector,
              bool isNewStart,
              uint32_t amonitorPort, bool withBuffer) override;

    bool init(const std::string &applicationId,
              const std::string &hippoZk,
              const std::string &carbonZk,
              const std::string &backupRoot,
              worker_framework::LeaderElector *leaderElector,
              const std::string &proxySpec = "", const std::string &kmonSpec = "",
              const DriverOptions& opts = DriverOptions());

    bool start() override;

    bool stop() override;

    void updateGroupPlans(const GroupPlanMap &groupPlans, ErrorInfo *errorInfo) override;

    void getGroupStatus(
            const std::vector<groupid_t> &groupids,
            std::vector<GroupStatus> *allGroupStatus) const override;

    void getAllGroupStatus(std::vector<GroupStatus> *allGroupStatus) const override;

    ::google::protobuf::Service* getService() const override;
    ::google::protobuf::Service* getOpsService() const override;

    virtual bool releaseSlots(
        const std::vector<SlotId> &slotIds, 
        uint64_t leaseMs, ErrorInfo *errorInfo) const override;

    virtual bool releaseIps(
        const std::vector<std::string> &ips, 
        uint64_t leaseMs, ErrorInfo *errorInfo) const override;

    GlobalConfigManagerPtr getGlobalConfigManager() const {
        return _carbonDriverPtr->getGlobalConfigManager();
    }

protected : 
    bool reclaimWorkerNodes(
        const std::vector<ReclaimWorkerNode> &workerNodeNames,
        uint64_t leaseMs, ErrorInfo *errorInfo) const;

private:
    CarbonDriverPtr _carbonDriverPtr;
    CarbonDriverCachePtr _driverCachePtr;
    SlotWorkerNodeManagerPtr _slotWorkerNodeManagerPtr;
    service::CarbonMasterServiceImpl *_carbonMasterService;
    service::CarbonOpsServiceImpl *_carbonOpsService;

private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CarbonDriverWrapperImpl);

END_CARBON_NAMESPACE(master);

#endif //CARBON_CARBONDRIVERWRAPPERIMPL_H
