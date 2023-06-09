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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "build_service/plugin/ModuleInfo.h"
#include "ha3/config/SummaryProfileInfo.h"

namespace isearch {
namespace config {

class SummaryProfileConfig : public autil::legacy::Jsonizable
{
public:
    SummaryProfileConfig();
    ~SummaryProfileConfig();
private:
    SummaryProfileConfig(const SummaryProfileConfig &);
    SummaryProfileConfig& operator=(const SummaryProfileConfig &);
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    const build_service::plugin::ModuleInfos& getModuleInfos() const {
        return _modules;
    }
    const SummaryProfileInfos& getSummaryProfileInfos() const {
        return _summaryProfileInfos;
    }
    const std::vector<std::string>& getRequiredAttributeFields() const {
        return _attributeFields;
    }
public:
    // for test
    void addRequiredAttributeField(const std::string &fieldName) {
        _attributeFields.push_back(fieldName);
    }
private:
    build_service::plugin::ModuleInfos _modules;
    SummaryProfileInfos _summaryProfileInfos;
    std::vector<std::string> _attributeFields;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryProfileConfig> SummaryProfileConfigPtr;

} // namespace config
} // namespace isearch

