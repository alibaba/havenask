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
#include "sdk/default/DefaultSlotAllocator.h"

using namespace std;
using namespace autil;
BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, DefaultSlotAllocator);

DefaultSlotAllocator::DefaultSlotAllocator(EventTrigger *eventTrigger,
        DefaultSlotAssigner *slotAssigner)
    : SlotAllocatorBase(eventTrigger)
    , _slotAssigner(slotAssigner)
{
}

DefaultSlotAllocator::~DefaultSlotAllocator() {
}

void DefaultSlotAllocator::updateResourceRequest(const std::string &role,
        const hippo::RoleRequest &request)
{
    hippo::RoleRequest newRequest = request;
    newRequest.context.preDeployPkgs.clear();
    newRequest.context.datas.clear();
    SlotAllocatorBase::updateResourceRequest(role, newRequest);
}

bool DefaultSlotAllocator::doAllocate(const proto::AllocateRequest &request,
                                      proto::AllocateResponse *response)
{
    if (_slotAssigner) {
        return _slotAssigner->assign(request, response);
    }
    return false;
}

void DefaultSlotAllocator::getAllSlotIds(map<string, set<SlotId> > *slotIds,
        map<SlotId, LaunchMeta> *launchMetas,
        map<hippo::SlotId, hippo::SlotResource> *slotResources)
{
    assert(slotIds);
    assert(launchMetas);
    assert(slotResources);
    ScopedLock lock(_mutex);
    for (RoleSlotInfoMap::const_iterator it = _allocatedSlots.begin();
         it != _allocatedSlots.end(); ++it)
    {
        const string &role = it->first;
        const SlotInfoMap &slotInfos = it->second;
        for (SlotInfoMap::const_iterator it2 = slotInfos.begin();
             it2 != slotInfos.end(); ++it2)
        {
            (*slotIds)[role].insert(it2->first);
            (*launchMetas)[it2->first] = LaunchMeta(it2->second->requirementId,
                    it2->second->launchSignature);
            (*slotResources)[it2->first] = it2->second->slotResource;
        }
    }
}

void DefaultSlotAllocator::setLaunchedMetas(
        const map<hippo::SlotId, LaunchMeta> &launchedMetas)
{
    if (_slotAssigner) {
        _slotAssigner->setLaunchedMetas(launchedMetas);
    }
}

END_HIPPO_NAMESPACE(sdk);
