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
#include "swift/config/SwiftConfig.h"

#include <iosfwd>

using namespace std;
namespace swift {
namespace config {
AUTIL_LOG_SETUP(swift, SwiftConfig);

SwiftConfig::SwiftConfig() : amonitorPort(DEFAULT_AMONITOR_PORT) {}

SwiftConfig::~SwiftConfig() {}

void SwiftConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("user_name", userName, userName);
    json.Jsonize("service_name", serviceName, serviceName);
    json.Jsonize("hippo_root", hippoRoot, hippoRoot);
    json.Jsonize("zookeeper_root", zkRoot, zkRoot);
    json.Jsonize("amonitor_port", amonitorPort, amonitorPort);
}

string SwiftConfig::getApplicationId() const { return userName + "_" + serviceName; }

bool SwiftConfig::validate() const {
    if (userName.empty()) {
        AUTIL_LOG(ERROR, "UserName can't be empty.");
        return false;
    }
    if (serviceName.empty()) {
        AUTIL_LOG(ERROR, "ServiceName can't be empty.");
        return false;
    }
    if (hippoRoot.empty()) {
        AUTIL_LOG(ERROR, "HippoRoot can't be empty.");
        return false;
    }
    if (zkRoot.empty()) {
        AUTIL_LOG(ERROR, "Zookeeper can't be empty.");
        return false;
    }
    return true;
}

} // namespace config
} // namespace swift
