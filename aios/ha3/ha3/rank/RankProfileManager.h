#ifndef ISEARCH_RANKPROFILEMANAGER_H
#define ISEARCH_RANKPROFILEMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/RankAttributeExpression.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/rank/RankProfile.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/config/RankProfileInfo.h>
#include <ha3/common/MultiErrorResult.h>

BEGIN_HA3_NAMESPACE(common);
class Request;
END_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(rank);

class RankProfileManager
{
public:
    RankProfileManager(const build_service::plugin::PlugInManagerPtr &plugInManagerPtr);
    ~RankProfileManager();
public:
    typedef std::map<std::string, RankProfile*> RankProfiles;
public:
    bool getRankProfile(const common::Request *request,
                        const RankProfile*& rankProfile,
                        const common::MultiErrorResultPtr& errorResult) const;
public:
    bool init(const config::ResourceReaderPtr &resourceReaderPtr,
              const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr,
              kmonitor::MetricsReporter *_metricsReporter);
    void mergeFieldBoostTable(const suez::turing::TableInfo &tableInfo);
    bool addRankProfile(RankProfile *rankProfile);
private:
    RankProfile* getRankProfile(const std::string &profileName) const;
private:
    static bool isRankRequiredInRequest(const common::Request *request);
    static std::string getRankProfileName(const common::Request *request);
    static bool needCreateRankProfile(const common::Request *request);
private:
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    RankProfiles _rankProfiles;
private:
    friend class RankProfileManagerTest;
    friend class RankProfileManagerCreatorTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RankProfileManager);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_RANKPROFILEMANAGER_H
