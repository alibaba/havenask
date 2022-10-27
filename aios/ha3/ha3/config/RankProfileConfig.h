#ifndef ISEARCH_RANKPROFILECONFIG_H
#define ISEARCH_RANKPROFILECONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <build_service/plugin/ModuleInfo.h>
#include <ha3/config/TypeDefine.h>
#include <ha3/config/RankProfileInfo.h>

BEGIN_HA3_NAMESPACE(config);

class RankProfileConfig : public autil::legacy::Jsonizable
{
public:
    RankProfileConfig();
    ~RankProfileConfig();
private:
    RankProfileConfig(const RankProfileConfig &);
    RankProfileConfig& operator=(const RankProfileConfig &);
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const RankProfileConfig &other) const;
public:
    const build_service::plugin::ModuleInfos& getModuleInfos() const {
        return _modules;
    }
    const RankProfileInfos& getRankProfileInfos() const {
        return _rankProfileInfos;
    }
private:
    build_service::plugin::ModuleInfos _modules;
    RankProfileInfos _rankProfileInfos;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RankProfileConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_RANKPROFILECONFIG_H
