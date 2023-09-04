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
#ifndef CARBON_CARBONDRIVERWRAPPER_H
#define CARBON_CARBONDRIVERWRAPPER_H

#include "carbon/CommonDefine.h"
#include "carbon/ErrorDefine.h"
#include "carbon/GroupPlan.h"
#include "carbon/Status.h"
#include "carbon/ServiceAdapterExtCreator.h"
#include "worker_framework/LeaderElector.h"
#include "google/protobuf/service.h"

namespace carbon {

class CarbonDriverWrapper {
public:
    static CarbonDriverWrapper* createCarbonDriverWrapper();
public:
    virtual ~CarbonDriverWrapper() {}

    virtual bool init(const std::string &applicationId,
                      const std::string &hippoZk,
                      const std::string &carbonZk,
                      const std::string &backupRoot,
                      worker_framework::LeaderElector *leaderElector,
                      bool isNewStart,
                      uint32_t amnitorPort, bool withBuffer, bool k8sMode = false) = 0;

    virtual bool start() = 0;

    virtual bool stop() = 0;

    virtual void updateGroupPlans(const GroupPlanMap &groupPlans, ErrorInfo *errorInfo) = 0;

    virtual void getGroupStatus(
            const std::vector<groupid_t> &groupids,
            std::vector<GroupStatus> *allGroupStatus) const = 0;

    virtual void getAllGroupStatus(std::vector<GroupStatus> *allGroupStatus) const = 0;

    virtual ::google::protobuf::Service* getService() const = 0;
    virtual ::google::protobuf::Service* getOpsService() const = 0;
    
    virtual void setExtServiceAdapterCreator(ServiceAdapterExtCreatorPtr creator) = 0;

    virtual bool releaseSlots(const std::vector<SlotId> &slotIds,
                              uint64_t leaseMs,
                              ErrorInfo *errorInfo) = 0;
};

}

#endif
