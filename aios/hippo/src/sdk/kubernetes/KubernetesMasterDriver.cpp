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
#include "sdk/kubernetes/KubernetesMasterDriver.h"
#include "sdk/kubernetes/KubernetesSlotAllocator.h"
#include "sdk/kubernetes/KubernetesProcessLauncher.h"

using namespace std;
BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, KubernetesMasterDriver);

KubernetesMasterDriver::KubernetesMasterDriver()
    : BasicMasterDriver()
{
    _slotAllocator = new KubernetesSlotAllocator(_eventTrigger);
    _processLauncher = new KubernetesProcessLauncher();
}

KubernetesMasterDriver::~KubernetesMasterDriver() {
    detach();
    DELETE_AND_SET_NULL_HIPPO(_amLeaderChecker);
    DELETE_AND_SET_NULL_HIPPO(_processLauncher);
    DELETE_AND_SET_NULL_HIPPO(_slotAllocator);
}

bool KubernetesMasterDriver::init(const string &hippoZkRoot,
                             const string &leaderElectRoot,
                             const string &appMasterAddr,
                             const string &applicationId)
{
    if (!BasicMasterDriver::init(hippoZkRoot, leaderElectRoot,
                                 appMasterAddr, applicationId))
    {
        return false;
    }
    HIPPO_LOG(INFO, "kubernetes master driver init success. hippoZkRoot:[%s], "
              "leaderElectRoot:[%s], appMasterAddr:[%s], appId:[%s]",
              hippoZkRoot.c_str(), leaderElectRoot.c_str(),
              appMasterAddr.c_str(), applicationId.c_str());
    return true;
}

bool KubernetesMasterDriver::init(
        const string &hippoZkRoot,
        worker_framework::LeaderChecker * leaderChecker,
        const string &applicationId)
{
    if (!BasicMasterDriver::init(hippoZkRoot, leaderChecker, applicationId))
    {
        return false;
    }
    HIPPO_LOG(INFO, "kubernetes master driver init success."
              "hippoZkRoot:[%s], appId:[%s]",
              hippoZkRoot.c_str(), applicationId.c_str());
    return true;
}

void KubernetesMasterDriver::afterLoop() {
    auto slotAllocator = dynamic_cast<KubernetesSlotAllocator *>(_slotAllocator);
    assert(slotAllocator);
    map<string, set<hippo::SlotId> > slotIds;
    map<hippo::SlotId, LaunchMeta> launchMetas;
    slotAllocator->getAllSlotIds(&slotIds, &launchMetas);
    _processLauncher->setLaunchMetas(launchMetas);
    _processLauncher->launch(slotIds);
}

END_HIPPO_NAMESPACE(sdk);
