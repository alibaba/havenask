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
#ifndef HIPPO_KUBERNETESPROCESSLAUNCHER_H
#define HIPPO_KUBERNETESPROCESSLAUNCHER_H

#include "util/Log.h"
#include "common/common.h"
#include "sdk/ProcessLauncherBase.h"
#include "sdk/C2ProxyClient.h"

BEGIN_HIPPO_NAMESPACE(sdk);

class KubernetesProcessLauncher : public ProcessLauncherBase
{
public:
    KubernetesProcessLauncher();
    virtual ~KubernetesProcessLauncher();
private:
    KubernetesProcessLauncher(const KubernetesProcessLauncher &);
    KubernetesProcessLauncher& operator=(const KubernetesProcessLauncher &);
public:
    void launch(const std::map<std::string, std::set<hippo::SlotId> > &slotIds) override;

    bool resetSlot(const hippo::SlotId &slotId) override;

    void setApplicationId(const std::string &appId) override {
        ProcessLauncherBase::setApplicationId(appId);
        if (_c2ProxyClient != NULL) {
            _c2ProxyClient->setApplicationId(appId);
        }
    }
protected:
    void asyncLaunchOneSlot(const std::string& role,
                            const hippo::SlotId &slotId,
                            const hippo::ProcessContext &context) override;
private:
    bool needLaunch(const hippo::SlotId &slotId,
                    const proto::ProcessLaunchContext &launchContext) const;
private:
    C2ProxyClient *_c2ProxyClient;
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(KubernetesProcessLauncher);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_KUBERNETESPROCESSLAUNCHER_H
