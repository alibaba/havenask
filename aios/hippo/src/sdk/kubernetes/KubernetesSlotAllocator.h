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
#ifndef HIPPO_KUBERNETESSLOTALLOCATOR_H
#define HIPPO_KUBERNETESSLOTALLOCATOR_H

#include "util/Log.h"
#include "common/common.h"
#include "sdk/SlotAllocatorBase.h"
#include "sdk/C2ProxyClient.h"

BEGIN_HIPPO_NAMESPACE(sdk);

class KubernetesSlotAllocator : public SlotAllocatorBase
{
public:
    KubernetesSlotAllocator(EventTrigger *eventTrigger);
    virtual ~KubernetesSlotAllocator();
private:
    KubernetesSlotAllocator(const KubernetesSlotAllocator &);
    KubernetesSlotAllocator& operator=(const KubernetesSlotAllocator &);
public:
    void getAllSlotIds(std::map<std::string, std::set<hippo::SlotId> > *slotIds) const;

    void getAllSlotIds(std::map<std::string, std::set<hippo::SlotId> > *slotIds,
                       std::map<hippo::SlotId, LaunchMeta> *launchMetas) const;

    void setApplicationId(const std::string &appId) {
        _applicationId = appId;
        if (_c2ProxyClient != NULL) {
            _c2ProxyClient->setApplicationId(appId);
        }
    }
protected:
    bool doAllocate(const proto::AllocateRequest &request,
                    proto::AllocateResponse *response);
private:
    C2ProxyClient *_c2ProxyClient;
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(KubernetesSlotAllocator);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_KUBERNETESSLOTALLOCATOR_H
