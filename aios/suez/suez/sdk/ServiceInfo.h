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
#pragma once

#include <stdint.h>
#include <string>

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"

namespace suez {

class ServiceInfo : public autil::legacy::Jsonizable {
public:
    ServiceInfo();
    ~ServiceInfo();

public:
    bool operator==(const ServiceInfo &info) const;
    bool operator!=(const ServiceInfo &info) const;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    uint32_t getPartCount() const;
    uint32_t getPartId() const;
    const std::string &getRoleName() const;
    const std::string &getZoneName() const;
    const autil::legacy::json::JsonMap &getCm2ConfigMap() const;
    std::string toString() const;
    const autil::legacy::json::JsonArray &getSubCm2ConfigArray() const;

private:
    uint32_t _partCount;
    uint32_t _partId;
    std::string _roleName;
    std::string _zoneName;
    autil::legacy::json::JsonMap _cm2ConfigMap;
    // new version cm2 config, expect a list of old _cm2ConfigMap
    autil::legacy::json::JsonArray _subCm2ConfigArray;
};

} // namespace suez
