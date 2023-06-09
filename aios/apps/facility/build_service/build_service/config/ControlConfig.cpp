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
#include "build_service/config/ControlConfig.h"

#include "build_service/config/ResourceReader.h"

using namespace std;

namespace build_service { namespace config {

class SingleClusterControlConfig : public autil::legacy::Jsonizable
{
public:
    SingleClusterControlConfig() {}
    ~SingleClusterControlConfig() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("is_inc_processor_exist", isIncProcessorExist);
        json.Jsonize("cluster_name", clusterName);
    }
    bool isIncProcessorExist = true;
    std::string clusterName;
};

void ControlConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("use_v2_index", _useIndexV2, _useIndexV2);
    json.Jsonize("is_inc_processor_exist", _isIncProcessorExistByDefault, _isIncProcessorExistByDefault);

    std::vector<SingleClusterControlConfig> clusterConfigs;
    if (json.GetMode() == FROM_JSON) {
        json.Jsonize("clusters", clusterConfigs, clusterConfigs);
        for (auto& singleConf : clusterConfigs) {
            if (_clusterInfoMap.find(singleConf.clusterName) != _clusterInfoMap.end()) {
                string errorInfo = "duplicated clusterName [" + singleConf.clusterName + "]";
                throw autil::legacy::ExceptionBase(errorInfo);
            }
            _clusterInfoMap.insert(make_pair(singleConf.clusterName, singleConf.isIncProcessorExist));
        }
    } else {
        for (auto& kv : _clusterInfoMap) {
            SingleClusterControlConfig singleConfig;
            singleConfig.clusterName = kv.first;
            singleConfig.isIncProcessorExist = kv.second;
            clusterConfigs.emplace_back(singleConfig);
        }
        if (!clusterConfigs.empty()) {
            json.Jsonize("clusters", clusterConfigs);
        }
    }
}

bool ControlConfig::isAllClusterNeedIncProcessor(const std::vector<std::string>& clusterNames) const
{
    if (clusterNames.empty()) {
        return _isIncProcessorExistByDefault;
    }

    for (auto& cluster : clusterNames) {
        if (!isIncProcessorExist(cluster)) {
            return false;
        }
    }
    return true;
}

bool ControlConfig::isAnyClusterNeedIncProcessor(const std::vector<std::string>& clusterNames) const
{
    if (clusterNames.empty()) {
        return _isIncProcessorExistByDefault;
    }

    for (auto& cluster : clusterNames) {
        if (isIncProcessorExist(cluster)) {
            return true;
        }
    }
    return false;
}

}} // namespace build_service::config
