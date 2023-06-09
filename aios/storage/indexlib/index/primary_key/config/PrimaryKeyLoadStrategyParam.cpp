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
#include "indexlib/index/primary_key/config/PrimaryKeyLoadStrategyParam.h"

#include "autil/StringUtil.h"
#include "indexlib/config/ConfigDefine.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, PrimaryKeyLoadStrategyParam);

const std::string PrimaryKeyLoadStrategyParam::COMBINE_SEGMENTS = "combine_segments";
const std::string PrimaryKeyLoadStrategyParam::MAX_DOC_COUNT = "max_doc_count";
const std::string PrimaryKeyLoadStrategyParam::LOOKUP_REVERSE = "lookup_reverse";
const std::string PrimaryKeyLoadStrategyParam::PARAM_SEP = ",";
const std::string PrimaryKeyLoadStrategyParam::KEY_VALUE_SEP = "=";

Status PrimaryKeyLoadStrategyParam::FromString(const string& paramStr)
{
    if (paramStr.empty()) {
        return Status::OK();
    }

    vector<vector<string>> params;
    StringUtil::fromString(paramStr, params, "=", ",");
    if (!ParseParams(params)) {
        AUTIL_LOG(ERROR, "primary key load param [%s] not legal", paramStr.c_str());
        return Status::InvalidArgs("primary key load param error");
    }
    return Status::OK();
}

Status PrimaryKeyLoadStrategyParam::CheckParam(const std::string& param)
{
    PrimaryKeyLoadStrategyParam loadParam;
    return loadParam.FromString(param);
}

bool PrimaryKeyLoadStrategyParam::ParseParams(const vector<vector<string>>& params)
{
    for (size_t i = 0; i < params.size(); i++) {
        const vector<string>& param = params[i];
        if (param.size() != 2) {
            return false;
        }
        if (!ParseParam(param[0], param[1])) {
            return false;
        }
    }
    return true;
}

bool PrimaryKeyLoadStrategyParam::ParseParam(const string& name, const string& value)
{
    if (name == COMBINE_SEGMENTS) {
        return StringUtil::fromString(value, _needCombineSegments);
    }
    if (name == MAX_DOC_COUNT) {
        return StringUtil::fromString(value, _maxDocCount);
    }
    if (name == LOOKUP_REVERSE) {
        if (_needLookupReverse) {
            return true;
        }
        return StringUtil::fromString(value, _needLookupReverse);
    }
    return false;
}

Status PrimaryKeyLoadStrategyParam::CheckEqual(const PrimaryKeyLoadStrategyParam& other) const
{
    CHECK_CONFIG_EQUAL(_loadMode, other._loadMode, "pk load mode not equal");
    CHECK_CONFIG_EQUAL(_maxDocCount, other._maxDocCount, "max doc count not equal");
    CHECK_CONFIG_EQUAL(_needCombineSegments, other._needCombineSegments, "need combine segments not equal");
    return Status::OK();
}

}} // namespace indexlib::config
