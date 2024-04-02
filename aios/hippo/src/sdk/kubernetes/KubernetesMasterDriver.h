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
#ifndef HIPPO_KUBERNETESMASTERDRIVER_H
#define HIPPO_KUBERNETESMASTERDRIVER_H

#include "util/Log.h"
#include "common/common.h"
#include "sdk/BasicMasterDriver.h"

BEGIN_HIPPO_NAMESPACE(sdk);

class KubernetesMasterDriver : public BasicMasterDriver {
public:
    KubernetesMasterDriver();
    virtual ~KubernetesMasterDriver();
private:
    KubernetesMasterDriver(const KubernetesMasterDriver &);
    KubernetesMasterDriver& operator=(const KubernetesMasterDriver &);
public:
    /* override */ bool init(const std::string &hippoZkRoot,
                             const std::string &leaderElectRoot,
                             const std::string &appMasterAddr,
                             const std::string &applicationId);
    /* override */ bool init(
            const std::string &hippoZkRoot,
            worker_framework::LeaderChecker * leaderChecker,
            const std::string &applicationId);
protected:
    virtual void afterLoop();

private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(KubernetesMasterDriver);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_KUBERNETESMASTERDRIVER_H
