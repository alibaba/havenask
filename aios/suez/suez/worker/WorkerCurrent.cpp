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
#include "suez/worker/WorkerCurrent.h"

#include "autil/MurmurHash.h"
#include "suez/table/InnerDef.h"

namespace suez {

WorkerCurrent::WorkerCurrent() : _targetVersion(INVALID_TARGET_VERSION) {}

void WorkerCurrent::setAddress(std::string arpcAddress, std::string httpAddress) {
    _arpcAddress = std::move(arpcAddress);
    _httpAddress = std::move(httpAddress);
}

const std::string &WorkerCurrent::getArpcAddress() const { return _arpcAddress; }
const std::string &WorkerCurrent::getHttpAddress() const { return _httpAddress; }

int64_t WorkerCurrent::getTargetVersion() const { return _targetVersion; }
void WorkerCurrent::setTargetVersion(int64_t targetVersion) { _targetVersion = targetVersion; }

const TableMetas &WorkerCurrent::getTableInfos() const { return _tableInfos; }
TableMetas *WorkerCurrent::getTableMetas() { return &_tableInfos; }
void WorkerCurrent::setTableInfos(const TableMetas &tableInfos) { _tableInfos = tableInfos; }

const BizMetas &WorkerCurrent::getBizInfos() const { return _bizInfos; }
void WorkerCurrent::setBizInfos(const BizMetas &bizInfos) { _bizInfos = bizInfos; }

const AppMeta &WorkerCurrent::getAppMeta() const { return _appInfo; }
void WorkerCurrent::setAppMeta(const AppMeta &appMeta) { _appInfo = appMeta; }

const autil::legacy::json::JsonMap &WorkerCurrent::getServiceInfo() const { return _serviceInfo; }
void WorkerCurrent::setServiceInfo(const autil::legacy::json::JsonMap &serviceInfo) { _serviceInfo = serviceInfo; }

const ServiceInfo &WorkerCurrent::getZoneInfo() const { return _zoneInfo; }
void WorkerCurrent::setZoneInfo(const ServiceInfo &zoneInfo) { _zoneInfo = zoneInfo; }

void WorkerCurrent::setError(const SuezError &error) { _errorInfo = error; }

void WorkerCurrent::toHeartbeatFormat(autil::legacy::json::JsonMap &current) const {
    current.clear();
    current["table_info"] = autil::legacy::ToJson(getTableInfos());
    current["biz_info"] = autil::legacy::ToJson(getBizInfos());
    current["app_info"] = autil::legacy::ToJson(getAppMeta());
    current["service_info"] = getServiceInfo();
    if (ERROR_NONE != _errorInfo) {
        current["error_info"] = autil::legacy::ToJson(_errorInfo);
    }
    if (INVALID_TARGET_VERSION != _targetVersion) {
        current["target_version"] = _targetVersion;
    }
}

uint64_t WorkerCurrent::calculateSignature() const {
    std::string str = autil::legacy::ToJsonString(getTableInfos());
    str += autil::legacy::ToJsonString(getBizInfos());
    str += autil::legacy::ToJsonString(getServiceInfo());
    return autil::MurmurHash::MurmurHash64A(str.c_str(), str.size(), 0);
}

} // namespace suez
