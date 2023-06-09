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
#include "suez/heartbeat/HeartbeatTarget.h"

#include <cstdint>
#include <iosfwd>
#include <limits>
#include <map>
#include <utility>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, HeartbeatTarget);

namespace suez {

HeartbeatTarget::HeartbeatTarget()
    : _targetVersion(INVALID_TARGET_VERSION)
    , _diskSize(numeric_limits<uint32_t>::max())
    , _cleanDisk(true)
    , _preload(false) {}

HeartbeatTarget::~HeartbeatTarget() {}

void HeartbeatTarget::Jsonize(JsonWrapper &json) {
    json.Jsonize("app_info", _appMeta, _appMeta);
    json.Jsonize("biz_info", _bizMetas, _bizMetas);
    json.Jsonize("table_info", _tableMetas, _tableMetas);
    json.Jsonize("service_info", _serviceInfo, _serviceInfo);
    json.Jsonize("deploy_config", _deployConfig, _deployConfig);
    json.Jsonize("target_version", _targetVersion, _targetVersion);
    json.Jsonize("clean_disk", _cleanDisk, _cleanDisk);
    json.Jsonize("custom_app_info", _customAppInfo, _customAppInfo);
    json.Jsonize("disk_size", _diskSize, _diskSize);
    json.Jsonize("catalog_address", _catalogAddress, _catalogAddress);
}

bool HeartbeatTarget::parseFrom(const std::string &targetStr, const std::string &signature, bool preload) {
    if (targetStr.empty()) {
        AUTIL_LOG(DEBUG, "targetStr is empty");
        return false;
    }
    try {
        FromJsonString(_target, targetStr);
        FromJson(*this, _target);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "e[%s], target str[%s] jsonize failed", e.what(), targetStr.c_str());
        return false;
    }
    if (!checkTarget()) {
        AUTIL_LOG(WARN, "target check failed, target str[%s]", targetStr.c_str());
        return false;
    }
    _signature = signature;
    _preload = preload;

    return true;
}

bool HeartbeatTarget::checkTarget() const {
    bool ret = true;
    for (const auto &t : _tableMetas) {
        if (!t.second.validate()) {
            AUTIL_LOG(ERROR,
                      "table target validate failed, pid[%s], meta[%s]",
                      ToJsonString(t.first).c_str(),
                      ToJsonString(t.second).c_str());
            ret = false;
        }
    }
    if (!ret) {
        AUTIL_LOG(ERROR, "invalid target [%s]", ToJsonString(_target).c_str());
    }
    return ret;
}

} // namespace suez
