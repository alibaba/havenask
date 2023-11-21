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

#include <assert.h>
#include <iosfwd>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, ControlConfig);
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

bool ControlConfig::parseDataLinkStr(const std::string& inStr, DataLinkMode& dataLinkMode)
{
    if (inStr == "normal" || inStr == "NORMAL" || inStr.empty()) {
        dataLinkMode = DataLinkMode::NORMAL_MODE;
    } else if (inStr == "npc" || inStr == "NPC") {
        dataLinkMode = DataLinkMode::NPC_MODE;
    } else if (inStr == "fp_inp" || inStr == "FP_INP") {
        dataLinkMode = DataLinkMode::FP_INP_MODE;
    } else {
        return false;
    }
    return true;
}

string ControlConfig::dataLinkModeToStr(DataLinkMode dataLinkMode)
{
    switch (dataLinkMode) {
    case DataLinkMode::NORMAL_MODE:
        return "normal";
    case DataLinkMode::NPC_MODE:
        return "npc";
    case DataLinkMode::FP_INP_MODE:
        return "fp_inp";
    }
    return "";
}

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
        string dataLinkStr;
        json.Jsonize("data_link_mode", dataLinkStr, "");
        DataLinkMode mode;
        if (!parseDataLinkStr(dataLinkStr, mode)) {
            string errorInfo = "invalid data_link_mode [" + dataLinkStr + "]";
            throw autil::legacy::ExceptionBase(errorInfo);
        }
        switch (mode) {
        case DataLinkMode::FP_INP_MODE:
        case DataLinkMode::NPC_MODE: {
            _dataLinkMode = mode;
            _isIncProcessorExistByDefault = false;
            BS_LOG(INFO, "data_link_mode: %s will overwrite is_inc_processor_exist to false", dataLinkStr.c_str());
            break;
        }
        case DataLinkMode::NORMAL_MODE: {
            if (_isIncProcessorExistByDefault == false) {
                _dataLinkMode = DataLinkMode::FP_INP_MODE;
                BS_LOG(INFO, "legacy config is_inc_processor_exist = false will overwrite data_link_mode to bfp_inp");
            }
            break;
        }
        }
    } else {
        string dataLinkStr = dataLinkModeToStr(_dataLinkMode);
        json.Jsonize("data_link_mode", dataLinkStr);
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
ControlConfig::DataLinkMode ControlConfig::getTransferedDataLinkMode(const std::string& clusterName) const
{
    if (_dataLinkMode == DataLinkMode::NORMAL_MODE || _dataLinkMode == DataLinkMode::NPC_MODE) {
        return _dataLinkMode;
    }
    assert(_dataLinkMode == DataLinkMode::FP_INP_MODE);
    if (isIncProcessorExist(clusterName)) {
        return DataLinkMode::NORMAL_MODE;
    }
    return _dataLinkMode;
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
