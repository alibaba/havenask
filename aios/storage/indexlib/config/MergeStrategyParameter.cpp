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
#include "indexlib/config/MergeStrategyParameter.h"

#include <vector>

#include "autil/StringUtil.h"

namespace indexlibv2::config {

MergeStrategyParameter::MergeStrategyParameter() : _impl(std::make_unique<MergeStrategyParameter::Impl>()) {}

MergeStrategyParameter::MergeStrategyParameter(const MergeStrategyParameter& other)
    : _impl(std::make_unique<MergeStrategyParameter::Impl>(*other._impl))
{
}

std::string MergeStrategyParameter::GetStrategyConditions() const { return _impl->strategyConditions; }

MergeStrategyParameter& MergeStrategyParameter::operator=(const MergeStrategyParameter& other)
{
    if (this != &other) {
        _impl = std::make_unique<MergeStrategyParameter::Impl>(*other._impl);
    }
    return *this;
}

MergeStrategyParameter::~MergeStrategyParameter() {}

void MergeStrategyParameter::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("input_limits", _impl->inputLimitParam, _impl->inputLimitParam);
    json.Jsonize("strategy_conditions", _impl->strategyConditions, _impl->strategyConditions);
    json.Jsonize("output_limits", _impl->outputLimitParam, _impl->outputLimitParam);

    Check();
}

void MergeStrategyParameter::Check() const {}

void MergeStrategyParameter::SetLegacyString(const std::string& legacyStr)
{
    _impl->inputLimitParam = "";
    _impl->strategyConditions = legacyStr;
    _impl->outputLimitParam = "";
}

std::string MergeStrategyParameter::GetLegacyString() const
{
    std::vector<std::string> strVec;
    if (!_impl->inputLimitParam.empty()) {
        strVec.push_back(_impl->inputLimitParam);
    }

    if (!_impl->strategyConditions.empty()) {
        strVec.push_back(_impl->strategyConditions);
    }

    if (!_impl->outputLimitParam.empty()) {
        strVec.push_back(_impl->outputLimitParam);
    }

    if (strVec.empty()) {
        return std::string("");
    }
    return autil::StringUtil::toString(strVec, ";");
}

MergeStrategyParameter MergeStrategyParameter::CreateFromLegacyString(const std::string& paramStr)
{
    MergeStrategyParameter param;
    param.SetLegacyString(paramStr);
    return param;
}

MergeStrategyParameter MergeStrategyParameter::Create(const std::string& inputLimit,
                                                      const std::string& strategyConditions,
                                                      const std::string& outputLimit)
{
    MergeStrategyParameter param;
    param._impl->inputLimitParam = inputLimit;
    param._impl->strategyConditions = strategyConditions;
    param._impl->outputLimitParam = outputLimit;
    return param;
}

} // namespace indexlibv2::config
