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
#include "ha3/common/VersionCalculator.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/HashAlgorithm.h"

#include "autil/Log.h"
#include "ha3/version.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, VersionCalculator);

uint32_t VersionCalculator::calcVersion(const std::string &workerConfigVersion,
                                        const std::string &configDataVersion,
                                        const std::string &remoteConfigPath,
                                        const std::string &fullVersionStr)
{
    string delim(":");
    string versionStr;
    if (!workerConfigVersion.empty()) {
        versionStr = HA_BUILD_SIGNATURE + delim + workerConfigVersion;
    } else if (!configDataVersion.empty()) {
        versionStr = HA_BUILD_SIGNATURE + delim + fullVersionStr + delim + configDataVersion;
    } else {
        versionStr = HA_BUILD_SIGNATURE + delim + fullVersionStr + delim + remoteConfigPath;
    }
    uint32_t version = autil::HashAlgorithm::hashString(versionStr.c_str(),
            versionStr.size(), 0);
    AUTIL_LOG(INFO, "ha3 version str: [%s], version [%u]",
            versionStr.c_str(), version);
    return version;
}

} // namespace common
} // namespace isearch
