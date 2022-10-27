#include <ha3/rank/RankProfileManager.h>
#include <build_service/plugin/Module.h>
#include <ha3/rank/DefaultScorer.h>
#include <suez/turing/expression/plugin/ScorerModuleFactory.h>
#include <ha3/rank/RankProfile.h>
#include <ha3/common/Request.h>
#include <assert.h>
#include <string>
#include <limits>


using namespace std;
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(rank);
using namespace build_service::plugin;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, RankProfileManager);

RankProfileManager::RankProfileManager(const PlugInManagerPtr &plugInManagerPtr)
    : _plugInManagerPtr(plugInManagerPtr)
{
}

RankProfileManager::~RankProfileManager() {
    for (RankProfiles::iterator it = _rankProfiles.begin();
         it != _rankProfiles.end(); it++)
    {
        delete it->second;
    }
}

bool RankProfileManager::init(const config::ResourceReaderPtr &resourceReaderPtr,
                              const CavaPluginManagerPtr &cavaPluginManagerPtr,
                              kmonitor::MetricsReporter *metricsReporter)
{
    HA3_LOG(TRACE3, "Init rankprofiles, num [%ld]", _rankProfiles.size());
    for (RankProfiles::iterator it = _rankProfiles.begin();
         it != _rankProfiles.end(); it++)
    {
        RankProfile *rankProfile = it->second;
        if (!rankProfile->init(_plugInManagerPtr, resourceReaderPtr,
                               cavaPluginManagerPtr, metricsReporter))
        {
            HA3_LOG(ERROR, "Init rankprofile [%s] failed!", it->first.c_str());
            return false;
        }
    }

    return true;
}

void RankProfileManager::mergeFieldBoostTable(const TableInfo &tableInfo) {
    for (RankProfiles::iterator it = _rankProfiles.begin();
         it != _rankProfiles.end(); it++)
    {
        RankProfile *rankProfile = it->second;
        rankProfile->mergeFieldBoostTable(tableInfo);
    }
}

RankProfile* RankProfileManager::getRankProfile(const string &profileName) const {
    RankProfiles::const_iterator it = _rankProfiles.find(profileName);
    if (it == _rankProfiles.end()) {
        return NULL;
    }
    return it->second;
}

bool RankProfileManager::addRankProfile(RankProfile *rankProfile) {
    HA3_LOG(TRACE3, "RankProfileManager addRankProfile. RankProfileName:[%s]",
        rankProfile->getProfileName().c_str());

    _rankProfiles.insert(make_pair(rankProfile->getProfileName(), rankProfile));
    return true;
}

bool RankProfileManager::getRankProfile(const common::Request *request,
        const RankProfile*& rankProfile, const common::MultiErrorResultPtr& errorResult) const
{
    if (!needCreateRankProfile(request)) {
        return true;
    }
    std::string profileName = DEFAULT_RANK_PROFILE;
    const common::ConfigClause *configClause = request->getConfigClause();
    if (!configClause->getDebugQueryKey().empty()) {
        profileName = DEFAULT_DEBUG_RANK_PROFILE; //for debug query match
    } else {
        std::string specifiedProfileName = getRankProfileName(request);
        if (!specifiedProfileName.empty()) {
            profileName = specifiedProfileName;
        }
    }
    rankProfile = getRankProfile(profileName);
    if (!rankProfile) {
        std::string errorMsg = std::string("Get rank profile failed, profileName=")
                               + profileName;
        errorResult->addError(ERROR_NO_RANKPROFILE, errorMsg);
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool RankProfileManager::needCreateRankProfile(const common::Request *request) {
    std::string rankProfileName = getRankProfileName(request);
    if (request->getConfigClause()->needRerank() ||
        isRankRequiredInRequest(request) || !rankProfileName.empty()) {
        return true;
    }
    return false;
}

bool RankProfileManager::isRankRequiredInRequest(const common::Request *request)  {
    common::SortClause *sortClause = request->getSortClause();
    common::RankSortClause *rankSortClause = request->getRankSortClause();

    if (!sortClause && !rankSortClause) {
        return true;
    }
    if (sortClause) {
        const std::vector<common::SortDescription *> &sortDescriptions
            = sortClause->getSortDescriptions();
        for (std::vector<common::SortDescription *>::const_iterator it = sortDescriptions.begin();
             it != sortDescriptions.end(); it++)
        {
            if ((*it)->isRankExpression()) {
                return true;
            }
        }
    }
    if (rankSortClause) {
        uint32_t rankSortDescCount = rankSortClause->getRankSortDescCount();
        for (size_t i = 0; i < rankSortDescCount; ++i) {
            const std::vector<common::SortDescription*> &sortDescriptions =
                rankSortClause->getRankSortDesc(i)->getSortDescriptions();
            for (std::vector<common::SortDescription *>::const_iterator it = sortDescriptions.begin();
                 it != sortDescriptions.end(); it++) {
                if ((*it)->isRankExpression()) {
                    return true;
                }
            }
        }
    }
    return false;
}

std::string RankProfileManager::getRankProfileName(const common::Request *request) {
    std::string rankProfileName;
    common::RankClause *rankClause = request->getRankClause();
    if (rankClause) {
        rankProfileName = rankClause->getRankProfileName();
    }
    return rankProfileName;
}

END_HA3_NAMESPACE(rank);
