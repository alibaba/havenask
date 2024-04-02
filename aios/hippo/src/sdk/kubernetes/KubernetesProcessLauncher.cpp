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
#include "sdk/kubernetes/KubernetesProcessLauncher.h"
#include "hippo/ProtoWrapper.h"
#include "hippo/HippoUtil.h"
#include "util/SignatureUtil.h"

using namespace std;
USE_HIPPO_NAMESPACE(util);
BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, KubernetesProcessLauncher);

KubernetesProcessLauncher::KubernetesProcessLauncher()
    : ProcessLauncherBase()
{
    _c2ProxyClient = C2ProxyClientCreator::createC2ProxyClient();
}

KubernetesProcessLauncher::~KubernetesProcessLauncher() {
    DELETE_AND_SET_NULL_HIPPO(_c2ProxyClient);
}

void KubernetesProcessLauncher::launch(const map<string, set<hippo::SlotId> > &slotIds) {
    clearUselessContexts(slotIds);
    asyncLaunch(slotIds);
}

void KubernetesProcessLauncher::asyncLaunchOneSlot(
        const std::string& role,
        const hippo::SlotId &slotId,
        const hippo::ProcessContext &context)
{
    if (_c2ProxyClient == nullptr) {
        HIPPO_LOG(ERROR, "launch slot failed, k8s proxy is null.");
        return;
    }
    proto::ProcessLaunchRequest request;
    proto::ProcessLaunchResponse response;
    request.set_applicationid(_applicationId);
    request.set_appchecksum(_appChecksum);
    request.set_slotid(slotId.id);
    request.set_k8snamespace(slotId.k8sNamespace);
    request.set_k8spodname(slotId.k8sPodName);
    proto::ProcessLaunchContext *launchContext =
        request.mutable_processcontext();
    ProtoWrapper::convert(context, launchContext);
    if (!needLaunch(slotId, *launchContext)) {
        return;
    }
    request.set_launchsignature(SignatureUtil::signature(*launchContext));
    request.set_packagechecksum(hippo::HippoUtil::genPackageChecksum(context.pkgs));
    if (needLaunch(slotId, *launchContext)) {
        bool ok = _c2ProxyClient->launch(request, response);
        HIPPO_LOG(INFO, "start process on slot %s/%s, ok:[%d]",
                  slotId.k8sNamespace.c_str(), slotId.k8sPodName.c_str(), ok);
        HIPPO_LOG(DEBUG, "start process request[%s]", request.ShortDebugString().c_str());
    }
    return;
}

bool KubernetesProcessLauncher::needLaunch(const hippo::SlotId &slotId,
                                      const proto::ProcessLaunchContext &launchContext) const
{
    map<hippo::SlotId, LaunchMeta>::const_iterator it = _launchMetas.find(slotId);
    if (it == _launchMetas.end()) {
        return true;
    }

    int64_t launchSignature = SignatureUtil::signature(launchContext);
    return launchSignature != it->second.launchSignature;
}

bool KubernetesProcessLauncher::resetSlot(const hippo::SlotId &slotId) {
    HIPPO_LOG(ERROR, "kubernetes do not support reset slot.");
    return false;
}

END_HIPPO_NAMESPACE(sdk);
