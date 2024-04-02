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
#include "sdk/kubernetes/KubernetesSlotAllocator.h"

using namespace std;
using namespace autil;
BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, KubernetesSlotAllocator);

KubernetesSlotAllocator::KubernetesSlotAllocator(EventTrigger *eventTrigger)
    : SlotAllocatorBase(eventTrigger)
{
    _c2ProxyClient = C2ProxyClientCreator::createC2ProxyClient();
}

KubernetesSlotAllocator::~KubernetesSlotAllocator() {
    DELETE_AND_SET_NULL_HIPPO(_c2ProxyClient);
}

bool KubernetesSlotAllocator::doAllocate(const proto::AllocateRequest &request,
                               proto::AllocateResponse *response)
{
    if (_c2ProxyClient != NULL) {
        bool ok = _c2ProxyClient->allocate(request, *response);
        return ok;
    }
    HIPPO_LOG(ERROR, "k8s proxy is null.");
    return false;
}

void KubernetesSlotAllocator::getAllSlotIds(map<string, set<SlotId> > *slotIds) const
{
    assert(slotIds);
    map<hippo::SlotId, LaunchMeta> launchMetas;
    getAllSlotIds(slotIds, &launchMetas);
}

void KubernetesSlotAllocator::getAllSlotIds(map<string, set<SlotId> > *slotIds,
                                  map<SlotId, LaunchMeta> *launchMetas) const
{
    assert(slotIds);
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
        }
    }
}

END_HIPPO_NAMESPACE(sdk);
