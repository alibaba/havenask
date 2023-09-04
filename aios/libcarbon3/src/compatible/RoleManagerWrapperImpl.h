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
#ifndef CARBON_ROLEMANAGERWRAPPERIMPL_H
#define CARBON_ROLEMANAGERWRAPPERIMPL_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/legacy/RoleManagerWrapperInternal.h"

namespace carbon {
class CarbonDriverWrapper;
}

BEGIN_CARBON_NAMESPACE(compatible);

class RoleManagerWrapperImpl : public RoleManagerWrapper
{
public:
    RoleManagerWrapperImpl();
    ~RoleManagerWrapperImpl();
private:
    RoleManagerWrapperImpl(const RoleManagerWrapperImpl &);
    RoleManagerWrapperImpl& operator=(const RoleManagerWrapperImpl &);
public:
    bool init(const std::string &appId, const std::string &hippoZkPath,
              const std::string &amZkPath, bool isNewStart,
              worker_framework::LeaderElector *leaderElector,
              uint32_t amonitorPort);
    bool start();
    
    bool stop();

    bool setGroups(const std::map<std::string, GroupDesc> &groups);
    
    void getGroupStatuses(
            const std::vector<groupid_t> &groupids,
            std::map<std::string, GroupStatusWrapper> &groupStatuses);

    void getGroupStatuses(
            std::map<std::string, GroupStatusWrapper> &groupStatuses);
    
    bool cancelRollingUpdate(const std::string &groupName,
                             bool force = false, bool keepNewVersion = false)
    {
        assert(false);
        return true;
    }

    bool releseSlots(
            const std::vector<hippo::proto::SlotId> &slotIds,
            const hippo::proto::PreferenceDescription &preference)
    {
        assert(false);
        return true;
    }

    ::google::protobuf::Service* getService() const;
    ::google::protobuf::Service* getOpsService() const;

private:
    CarbonDriverWrapper* _carbonDriverWrapper;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(RoleManagerWrapperImpl);

END_CARBON_NAMESPACE(compatible);

#endif //CARBON_ROLEMANAGERWRAPPERIMPL_H
