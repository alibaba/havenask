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
#include "suez/sdk/ServiceInfo.h"

#include <cstdint>
#include <iosfwd>
#include <map>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"

using namespace std;
using namespace autil;
using namespace autil::legacy::json;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, ServiceInfo);

ServiceInfo::ServiceInfo() : _partCount(1), _partId(0) {}

ServiceInfo::~ServiceInfo() {}

bool ServiceInfo::operator==(const ServiceInfo &info) const {
    return this->_partId == info._partId && this->_partCount == info._partCount && this->_roleName == info._roleName &&
           this->_zoneName == info._zoneName && ToJsonString(this->_cm2ConfigMap) == ToJsonString(info._cm2ConfigMap) &&
           ToJsonString(this->_subCm2ConfigArray) == ToJsonString(info._subCm2ConfigArray);
}

bool ServiceInfo::operator!=(const ServiceInfo &info) const { return !(*this == info); }

void ServiceInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("part_count", _partCount, _partCount);
    json.Jsonize("part_id", _partId, _partId);
    json.Jsonize("role_name", _roleName, _roleName);
    json.Jsonize("zone_name", _zoneName, _zoneName);
    json.Jsonize("cm2_config", _cm2ConfigMap, _cm2ConfigMap);
    json.Jsonize("sub_cm2_configs", _subCm2ConfigArray, _subCm2ConfigArray);
    if (_partCount == 0) {
        _partCount = 1;
    }
}

uint32_t ServiceInfo::getPartCount() const { return _partCount; }

uint32_t ServiceInfo::getPartId() const { return _partId; }

const string &ServiceInfo::getRoleName() const { return _roleName; }

const string &ServiceInfo::getZoneName() const { return _zoneName; }

const JsonMap &ServiceInfo::getCm2ConfigMap() const { return _cm2ConfigMap; }

std::string ServiceInfo::toString() const {
    return _zoneName + "_" + _roleName + "_" + StringUtil::toString(_partCount) + "_" + StringUtil::toString(_partId);
}

const JsonArray &ServiceInfo::getSubCm2ConfigArray() const { return _subCm2ConfigArray; }

} // namespace suez
