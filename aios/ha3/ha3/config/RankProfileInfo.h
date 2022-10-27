#ifndef ISEARCH_RANKPROFILEINFO_H
#define ISEARCH_RANKPROFILEINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <ha3/config/TypeDefine.h>

BEGIN_HA3_NAMESPACE(config);

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
    HA3_LOG_DECLARE();
};
typedef std::vector<RankProfileInfo> RankProfileInfos;

HA3_TYPEDEF_PTR(RankProfileInfo);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_RANKPROFILEINFO_H
