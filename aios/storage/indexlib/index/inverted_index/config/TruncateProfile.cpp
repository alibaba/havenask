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
#include "indexlib/index/inverted_index/config/TruncateProfile.h"
// #include "indexlib/index/inverted_index/Constant.h"
namespace indexlibv2::config {

AUTIL_LOG_SETUP(indexlib.config, TruncateProfile);
TruncateProfile::TruncateProfile(const TruncateProfileConfig& truncateProfileConfig)
{
    truncateProfileName = truncateProfileConfig.GetTruncateProfileName();
    sortParams = truncateProfileConfig.GetTruncateSortParams();
    docPayloadParams = truncateProfileConfig.GetDocPayloadParams();
}

void TruncateProfile::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("TruncateProfileName", truncateProfileName, truncateProfileName);
    json.Jsonize("IndexNames", indexNames, indexNames);
    json.Jsonize("TruncateStrategyName", truncateStrategyName, truncateStrategyName);
    json.Jsonize("TruncateDocPayloadParams", docPayloadParams, docPayloadParams);
    if (json.GetMode() == TO_JSON) {
        if (sortParams.size() > 1) {
            json.Jsonize("SortParams", sortParams, sortParams);
        } else if (sortParams.size() == 1) {
            std::string strSort = sortParams[0].GetSortField();
            json.Jsonize("SortField", strSort, strSort);
            strSort = sortParams[0].GetSortPatternString();
            json.Jsonize("SortType", strSort, strSort);
        }
    } else {
        std::string sortFieldName;
        std::string sortPattern = "DESC";
        json.Jsonize("SortField", sortFieldName, sortFieldName);
        json.Jsonize("SortType", sortPattern, sortPattern);

        if (!sortFieldName.empty()) {
            indexlib::config::SortParam tmpSortParam;
            tmpSortParam.SetSortField(sortFieldName);
            tmpSortParam.SetSortPatternString(sortPattern);
            sortParams.push_back(tmpSortParam);
        } else {
            std::vector<indexlib::config::SortParam> tmpSortParams;
            json.Jsonize("SortParams", tmpSortParams, tmpSortParams);
            for (size_t i = 0; i < tmpSortParams.size(); ++i) {
                if (tmpSortParams[i].GetSortField().empty()) {
                    continue;
                }
                sortParams.push_back(tmpSortParams[i]);
            }
        }
    }
}

bool TruncateProfile::Check() const
{
    if (truncateProfileName.empty()) {
        AUTIL_LOG(ERROR, "truncate profile name is invalid.");
        return false;
    }

    for (size_t i = 0; i < sortParams.size(); ++i) {
        if (!sortParams[i].GetSortField().empty()) {
            const std::string sortPattern = sortParams[i].GetSortPatternString();
            if (sortPattern != "DESC" && sortPattern != "ASC") {
                AUTIL_LOG(ERROR, "invalid sort pattern.");
                return false;
            }
        }
    }
    return true;
}

void TruncateProfile::AppendIndexName(const std::string& indexName)
{
    for (size_t i = 0; i < indexNames.size(); ++i) {
        if (indexNames[i] == indexName) {
            return;
        }
    }
    indexNames.push_back(indexName);
}

bool TruncateProfile::operator==(const TruncateProfile& other) const
{
    if (indexNames.size() != other.indexNames.size()) {
        return false;
    }

    if (sortParams.size() != other.sortParams.size()) {
        return false;
    }

    for (size_t i = 0; i < indexNames.size(); ++i) {
        if (indexNames[i] != other.indexNames[i]) {
            return false;
        }
    }

    for (size_t i = 0; i < sortParams.size(); ++i) {
        if (sortParams[i].GetSortField() != other.sortParams[i].GetSortField()) {
            return false;
        }

        if (sortParams[i].GetSortPattern() != other.sortParams[i].GetSortPattern()) {
            return false;
        }
    }
    if (docPayloadParams.size() != other.docPayloadParams.size()) {
        return false;
    }

    for (const auto& kv : docPayloadParams) {
        auto iter = other.docPayloadParams.find(kv.first);
        if (iter == other.docPayloadParams.end()) {
            return false;
        } else if (kv.second != iter->second) {
            return false;
        }
    }

    return truncateProfileName == other.truncateProfileName && truncateStrategyName == other.truncateStrategyName;
}

bool TruncateProfile::IsSortByDocPayload() const
{
    for (size_t i = 0; i < sortParams.size(); ++i) {
        if (TruncateProfileConfig::IsSortParamByDocPayload(sortParams[i])) {
            return true;
        }
    }
    return false;
}

std::string TruncateProfile::GetDocPayloadFactorField() const
{
    auto iter = docPayloadParams.find("DOC_PAYLOAD_FACTOR");
    if (iter == docPayloadParams.end()) {
        return "";
    } else {
        return iter->second;
    }
}

bool TruncateProfile::DocPayloadUseFP16() const
{
    auto iter = docPayloadParams.find("DOC_PAYLOAD_USE_FP_16");
    if (iter == docPayloadParams.end()) {
        return false;
    } else {
        return iter->second == "true";
    }
}

} // namespace indexlibv2::config
