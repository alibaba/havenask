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
#include "hippo/MasterDriver.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "common/common.h"
#include "sdk/default/DefaultMasterDriver.h"
#include "sdk/kubernetes/KubernetesMasterDriver.h"

using namespace std;

USE_HIPPO_NAMESPACE(sdk);

namespace hippo {

MasterDriver::MasterDriver() { }

MasterDriver::~MasterDriver() { }

MasterDriver* MasterDriver::createDriver(const std::string &driverName) {
    string scheduleMode = autil::EnvUtil::getEnv("HIPPO_SCHEDULE_MODE", "");
    if (scheduleMode == "kubernetes") {
        return new KubernetesMasterDriver();
    }
    return new DefaultMasterDriver();
}


};
