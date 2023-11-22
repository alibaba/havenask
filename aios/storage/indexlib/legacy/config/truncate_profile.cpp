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
#include "indexlib/config/truncate_profile.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/index/inverted_index/Constant.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, TruncateProfile);

TruncateProfile::TruncateProfile() {}

TruncateProfile::TruncateProfile(TruncateProfileConfig& truncateProfileConfig)
{
    mTruncateProfileName = truncateProfileConfig.GetTruncateProfileName();
    mSortParams = truncateProfileConfig.GetTruncateSortParams();
    mDocPayloadParams = truncateProfileConfig.GetDocPayloadParams();
}

TruncateProfile::~TruncateProfile() {}

void TruncateProfile::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("TruncateProfileName", mTruncateProfileName, mTruncateProfileName);
    json.Jsonize("IndexNames", mIndexNames, mIndexNames);
    json.Jsonize("TruncateStrategyName", mTruncateStrategyName, mTruncateStrategyName);
    json.Jsonize("TruncateDocPayloadParams", mDocPayloadParams, mDocPayloadParams);

    if (json.GetMode() == TO_JSON) {
        if (mSortParams.size() > 1) {
            json.Jsonize("SortParams", mSortParams, mSortParams);
        } else if (mSortParams.size() == 1) {
            std::string strSort = mSortParams[0].GetSortField();
            json.Jsonize("SortField", strSort, strSort);
            strSort = mSortParams[0].GetSortPatternString();
            json.Jsonize("SortType", strSort, strSort);
        }
    } else {
        string sortFieldName;
        string sortPattern = DESC_SORT_PATTERN;
        json.Jsonize("SortField", sortFieldName, sortFieldName);
        json.Jsonize("SortType", sortPattern, sortPattern);

        if (!sortFieldName.empty()) {
            SortParam tmpSortParam;
            tmpSortParam.SetSortField(sortFieldName);
            tmpSortParam.SetSortPatternString(sortPattern);
            mSortParams.push_back(tmpSortParam);
        } else {
            SortParams tmpSortParams;
            json.Jsonize("SortParams", tmpSortParams, tmpSortParams);
            for (size_t i = 0; i < tmpSortParams.size(); ++i) {
                if (tmpSortParams[i].GetSortField().empty()) {
                    continue;
                }
                mSortParams.push_back(tmpSortParams[i]);
            }
        }
    }
}

void TruncateProfile::Check() const
{
    bool valid = !mTruncateProfileName.empty();
    IE_CONFIG_ASSERT_PARAMETER_VALID(valid, "truncate profile name is invalid");

    for (size_t i = 0; i < mSortParams.size(); ++i) {
        if (!mSortParams[i].GetSortField().empty()) {
            std::string sortPattern = mSortParams[i].GetSortPatternString();
            valid = sortPattern == DESC_SORT_PATTERN || sortPattern == ASC_SORT_PATTERN;
            IE_CONFIG_ASSERT_PARAMETER_VALID(valid, "sort pattern is invalid");
        }
    }
}

void TruncateProfile::AppendIndexName(const std::string& indexName)
{
    for (size_t i = 0; i < mIndexNames.size(); ++i) {
        if (mIndexNames[i] == indexName) {
            return;
        }
    }
    mIndexNames.push_back(indexName);
}

bool TruncateProfile::operator==(const TruncateProfile& other) const
{
    if (mIndexNames.size() != other.mIndexNames.size()) {
        return false;
    }

    if (mSortParams.size() != other.mSortParams.size()) {
        return false;
    }

    for (size_t i = 0; i < mIndexNames.size(); ++i) {
        if (mIndexNames[i] != other.mIndexNames[i]) {
            return false;
        }
    }

    for (size_t i = 0; i < mSortParams.size(); ++i) {
        if (mSortParams[i].GetSortField() != other.mSortParams[i].GetSortField()) {
            return false;
        }

        if (mSortParams[i].GetSortPattern() != other.mSortParams[i].GetSortPattern()) {
            return false;
        }
    }

    if (mDocPayloadParams.size() != other.mDocPayloadParams.size()) {
        return false;
    }

    for (const auto& kv : mDocPayloadParams) {
        auto iter = other.mDocPayloadParams.find(kv.first);
        if (iter == other.mDocPayloadParams.end()) {
            return false;
        } else if (kv.second != iter->second) {
            return false;
        }
    }

    return mTruncateProfileName == other.mTruncateProfileName && mTruncateStrategyName == other.mTruncateStrategyName;
}

bool TruncateProfile::IsSortParamByDocPayload(const SortParam& sortParam)
{
    return sortParam.GetSortField() == DOC_PAYLOAD_FIELD_NAME;
}

bool TruncateProfile::IsSortByDocPayload() const
{
    for (size_t i = 0; i < mSortParams.size(); ++i) {
        if (IsSortParamByDocPayload(mSortParams[i])) {
            return true;
        }
    }
    return false;
}

string TruncateProfile::GetDocPayloadFactorField() const
{
    auto iter = mDocPayloadParams.find(DOC_PAYLOAD_FACTOR_FIELD_NAME);
    if (iter == mDocPayloadParams.end()) {
        return "";
    } else {
        return iter->second;
    }
}

bool TruncateProfile::DocPayloadUseFP16() const
{
    auto iter = mDocPayloadParams.find(DOC_PAYLOAD_USE_FP_16);
    if (iter == mDocPayloadParams.end()) {
        return false;
    } else {
        return iter->second == "true";
    }
}

}} // namespace indexlib::config
