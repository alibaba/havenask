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

#include <memory>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/index/inverted_index/config/TruncateProfileConfig.h"

namespace indexlibv2::config {

struct TruncateProfile : public autil::legacy::Jsonizable {
public:
    TruncateProfile() = default;
    TruncateProfile(const TruncateProfileConfig& truncateProfileConfig);
    ~TruncateProfile() = default;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool Check() const;
    void AppendIndexName(const std::string& indexName);
    bool operator==(const TruncateProfile& other) const;
    bool HasSort() const { return !sortParams.empty(); };
    size_t GetSortDimenNum() const { return sortParams.size(); }
    bool IsSortByDocPayload() const;
    bool DocPayloadUseFP16() const;
    std::string GetDocPayloadFactorField() const;

public:
    std::string truncateProfileName;
    std::vector<indexlib::config::SortParam> sortParams;
    std::string truncateStrategyName;
    std::vector<std::string> indexNames;
    std::map<std::string, std::string> docPayloadParams;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
