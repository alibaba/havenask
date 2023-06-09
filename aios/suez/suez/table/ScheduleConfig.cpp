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
#include "suez/table/ScheduleConfig.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "suez/sdk/CmdLineDefine.h"

using namespace autil;
namespace suez {

void ScheduleConfig::initFromEnv() {
    allowForceLoad = autil::EnvUtil::getEnv(RS_ALLOW_FORCELOAD, true);
    allowReloadByConfig = autil::EnvUtil::getEnv(RS_ALLOW_RELOAD_BY_CONFIG, false);
    allowReloadByIndexRoot = autil::EnvUtil::getEnv(RS_ALLOW_RELOAD_BY_INDEX_ROOT, false);
    allowPreload = autil::EnvUtil::getEnv("ALLOW_PRELOAD", false);
    allowFastCleanIncVersion = autil::EnvUtil::getEnv("FAST_CLEAN_INC_VERSION", false);
    fastCleanIncVersionIntervalInSec =
        autil::EnvUtil::getEnv("FAST_CLEAN_INC_VERSION_INTERVAL_IN_SEC", fastCleanIncVersionIntervalInSec);
    allowFollowerWrite = autil::EnvUtil::getEnv("ALLOW_FOLLOWER_WRITE", false);
    allowLoadUtilRtRecovered = autil::EnvUtil::getEnv("ALLOW_LOAD_UTIL_RT_RECOVERED", true);
}

static const char *boolToString(bool flag) { return flag ? "true" : "false"; }

std::string ScheduleConfig::toString() const {
    std::stringstream ss;
    ss << "allowForceLoad=" << boolToString(allowForceLoad) << ","
       << "allowReloadByConfig=" << boolToString(allowReloadByConfig) << ","
       << "allowReloadByIndexRoot=" << boolToString(allowReloadByIndexRoot) << ","
       << "allowPreload=" << boolToString(allowPreload) << ","
       << "allowFastCleanIncVersion=" << boolToString(allowFastCleanIncVersion) << ","
       << "fastCleanIncVersionIntervalInSec=" << StringUtil::toString(fastCleanIncVersionIntervalInSec) << ","
       << "allowFollowerWrite=" << boolToString(allowFollowerWrite) << ","
       << "allowLoadUtilRtRecovered=" << boolToString(allowLoadUtilRtRecovered);
    return ss.str();
}

} // namespace suez
