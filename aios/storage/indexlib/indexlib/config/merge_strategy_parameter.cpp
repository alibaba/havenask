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
#include "indexlib/config/merge_strategy_parameter.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, MergeStrategyParameter);

MergeStrategyParameter::MergeStrategyParameter() {}

MergeStrategyParameter::~MergeStrategyParameter() {}

void MergeStrategyParameter::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("input_limits", inputLimitParam, inputLimitParam);
    json.Jsonize("strategy_conditions", strategyConditions, strategyConditions);
    json.Jsonize("output_limits", outputLimitParam, outputLimitParam);
}

void MergeStrategyParameter::SetLegacyString(const string& legacyStr)
{
    inputLimitParam = "";
    strategyConditions = legacyStr;
    outputLimitParam = "";
}

string MergeStrategyParameter::GetLegacyString() const
{
    vector<string> strVec;
    if (!inputLimitParam.empty()) {
        strVec.push_back(inputLimitParam);
    }

    if (!strategyConditions.empty()) {
        strVec.push_back(strategyConditions);
    }

    if (!outputLimitParam.empty()) {
        strVec.push_back(outputLimitParam);
    }

    if (strVec.empty()) {
        return string("");
    }
    return StringUtil::toString(strVec, ";");
}
}} // namespace indexlib::config
