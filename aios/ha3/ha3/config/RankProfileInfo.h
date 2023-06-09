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

#include <stdint.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "ha3/isearch.h"

namespace isearch {
namespace config {

// ScorerInfo
class ScorerInfo : public autil::legacy::Jsonizable 
{
public:
    ScorerInfo();
    ~ScorerInfo();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const ScorerInfo &other) const;
public:
    std::string scorerName;
    std::string moduleName;
    KeyValueMap parameters;
    uint32_t rankSize;
    uint32_t totalRankSize;
};
typedef std::vector<ScorerInfo> ScorerInfos;

// RankProfileInfo
class RankProfileInfo : public autil::legacy::Jsonizable
{
public:
    RankProfileInfo();
    ~RankProfileInfo();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const RankProfileInfo &other) const;
public:
    std::string rankProfileName;
    ScorerInfos scorerInfos;
    std::map<std::string, uint32_t> fieldBoostInfo;
private:
    AUTIL_LOG_DECLARE();
};
typedef std::vector<RankProfileInfo> RankProfileInfos;

typedef std::shared_ptr<RankProfileInfo> RankProfileInfoPtr;

} // namespace config
} // namespace isearch

