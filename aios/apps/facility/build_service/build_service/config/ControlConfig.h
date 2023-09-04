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
#ifndef ISEARCH_BS_CONTROLCONFIG_H
#define ISEARCH_BS_CONTROLCONFIG_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {
class ResourceReader;

class ControlConfig : public autil::legacy::Jsonizable
{
public:
    ControlConfig() : _isIncProcessorExistByDefault(true) {}
    ~ControlConfig() {}

public:
    /**
        NORMAL_MODE: full-build with processor + inc build with processor
        NPC_MODE: disable processor-chain, and no processors in full-build and inc-build
        FP_INP_MODE: full-build with processor + inc build without processor, processor chain is running in builder
     **/
    enum struct DataLinkMode : int {
        NORMAL_MODE = 0,
        NPC_MODE = 1,
        FP_INP_MODE = 2,
    };
    static constexpr DataLinkMode DEFAULT_DATA_LINK_MODE = DataLinkMode::NORMAL_MODE;
    static bool parseDataLinkStr(const std::string& inStr, DataLinkMode& dataLinkMode);
    static std::string dataLinkModeToStr(DataLinkMode dataLinkMode);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    bool useIndexV2() const { return _useIndexV2; }
    bool isIncProcessorExist(const std::string& clusterName) const
    {
        auto iter = _clusterInfoMap.find(clusterName);
        if (iter != _clusterInfoMap.end()) {
            return iter->second;
        }
        return _isIncProcessorExistByDefault;
    }

    DataLinkMode getDataLinkMode() const { return _dataLinkMode; }
    bool isAllClusterNeedIncProcessor(const std::vector<std::string>& clusterNames) const;
    bool isAnyClusterNeedIncProcessor(const std::vector<std::string>& clusterNames) const;
    void setIsInc(bool isInc) { _isIncProcessorExistByDefault = isInc; }

private:
    bool _isIncProcessorExistByDefault = true;
    bool _useIndexV2 = false;
    DataLinkMode _dataLinkMode = DEFAULT_DATA_LINK_MODE;
    std::map<std::string, bool> _clusterInfoMap; /* KEY: clusterName, VALUE: if hasIncProcessor */

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::config

#endif // ISEARCH_BS_CONTROLCONFIG_H
