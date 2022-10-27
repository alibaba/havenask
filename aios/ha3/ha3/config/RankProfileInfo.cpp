#include <ha3/config/RankProfileInfo.h>
#include <ha3/config/RankSizeConverter.h>

using namespace std;
BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, RankProfileInfo);

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

END_HA3_NAMESPACE(config);

