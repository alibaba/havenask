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
#include "ha3/config/RankProfileInfo.h"

#include <cstdint>
#include <iosfwd>

#include "autil/legacy/jsonizable.h"

#include "ha3/config/RankSizeConverter.h"
#include "ha3/util/TypeDefine.h"
#include "ha3/isearch.h"
#include "autil/Log.h"

using namespace std;
namespace isearch {
namespace config {
AUTIL_LOG_SETUP(ha3, RankProfileInfo);

// ScorerInfo
ScorerInfo::ScorerInfo() {
    scorerName = "DefaultScorer";
    moduleName = "";
    rankSize = RankSizeConverter::convertToRankSize("UNLIMITED");
    totalRankSize = 0;
}

ScorerInfo::~ScorerInfo() {
}

void ScorerInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    string rankSizeStr = "UNLIMITED";
    string totalRankSizeStr;
    JSONIZE(json, "scorer_name", scorerName);
    JSONIZE(json, "module_name", moduleName);
    JSONIZE(json, "parameters", parameters);
    JSONIZE(json, "rank_size", rankSizeStr);
    JSONIZE(json, "total_rank_size", totalRankSizeStr);

    rankSize = RankSizeConverter::convertToRankSize(rankSizeStr);
    totalRankSize = RankSizeConverter::convertToRankSize(totalRankSizeStr);
}

bool ScorerInfo::operator==(const ScorerInfo &other) const {
    if (&other == this) {
        return true;
    }
    return scorerName == other.scorerName
        && moduleName == other.moduleName
        && rankSize == other.rankSize
        && totalRankSize == other.totalRankSize
        && parameters == other.parameters;
}

// RankProfileInfo
RankProfileInfo::RankProfileInfo() {
    rankProfileName = "DefaultProfile";
    scorerInfos.push_back(ScorerInfo());
}

RankProfileInfo::~RankProfileInfo() {
}

void RankProfileInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json){
    JSONIZE(json, "rank_profile_name", rankProfileName);
    JSONIZE(json, "scorers", scorerInfos);
    JSONIZE(json, "field_boost", fieldBoostInfo);
}

bool RankProfileInfo::operator==(const RankProfileInfo &other) const {
    if (&other == this) {
        return true;
    }
    return rankProfileName == other.rankProfileName
        && scorerInfos == other.scorerInfos
        && fieldBoostInfo == other.fieldBoostInfo;
}

} // namespace config
} // namespace isearch
