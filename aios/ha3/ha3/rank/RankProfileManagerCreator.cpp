#include <ha3/rank/RankProfileManagerCreator.h>
#include <suez/turing/common/FileUtil.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/config/RankSizeConverter.h>
#include <suez/turing/expression/plugin/ScorerModuleFactory.h>
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace std;
USE_HA3_NAMESPACE(util);
using namespace build_service::plugin;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, RankProfileManagerCreator);

RankProfileManagerCreator::RankProfileManagerCreator(
        const config::ResourceReaderPtr &resourceReaderPtr,
        const CavaPluginManagerPtr &cavaPluginManagerPtr,
        kmonitor::MetricsReporter *metricsReporter)
    : _resourceReaderPtr(resourceReaderPtr)
    , _cavaPluginManagerPtr(cavaPluginManagerPtr)
    , _metricsReporter(metricsReporter)
{
}

RankProfileManagerCreator::~RankProfileManagerCreator() {
}

void RankProfileManagerCreator::addRecordProfileInfo(
        RankProfileInfos& rankProfileInfos)
{
    string profileName = DEFAULT_DEBUG_RANK_PROFILE;
    string scorerName = DEFAULT_DEBUG_SCORER;
    for(int i = 0; i < (int)rankProfileInfos.size(); i++) {
        if(rankProfileInfos[i].rankProfileName == profileName) {
            rankProfileInfos.erase(rankProfileInfos.begin() + i);
            i--;
        }
    }
    ScorerInfo scorerInfo;
    scorerInfo.scorerName = scorerName;
    scorerInfo.rankSize = 10;
    RankProfileInfo profileInfo;
    profileInfo.rankProfileName = profileName;
    profileInfo.scorerInfos.clear();
    profileInfo.scorerInfos.push_back(scorerInfo);
    rankProfileInfos.push_back(profileInfo);
}

RankProfileManagerPtr RankProfileManagerCreator::createFromFile(
        const string &filePath)
{
    string configStr = FileUtil::readFile(filePath);
    return createFromString(configStr);
}

RankProfileManagerPtr RankProfileManagerCreator::createFromString(
        const std::string &configStr)
{
    RankProfileConfig rankProfileConfig;
    if (!configStr.empty()) {
        try {
            FromJsonString(rankProfileConfig, configStr);
        } catch (const ExceptionBase &e){
            HA3_LOG(ERROR, "Catch exception in RankProfileManagerCreator: [%s]\n"
                    " FromJsonString fail: [%s]", e.what(), configStr.c_str());
            return RankProfileManagerPtr();
        }
    }
    return create(rankProfileConfig);
}

RankProfileManagerPtr RankProfileManagerCreator::create(const RankProfileConfig &rankProfileConfig) {
    RankProfileInfos rankProfileInfos = rankProfileConfig.getRankProfileInfos();
    RankProfileManagerPtr rankProfileManagerPtr;
    PlugInManagerPtr plugInManagerPtr(new PlugInManager(
                    _resourceReaderPtr, suez::turing::MODULE_FUNC_SCORER));
    if (!plugInManagerPtr->addModules(rankProfileConfig.getModuleInfos())) {
        HA3_LOG(ERROR, "Load scorer module failed : %s", ToJsonString(rankProfileConfig.getModuleInfos()).c_str());
        return rankProfileManagerPtr;
    }
    rankProfileManagerPtr.reset(new RankProfileManager(plugInManagerPtr));
    addRecordProfileInfo(rankProfileInfos); //for analyze query match
    for (size_t i = 0; i < rankProfileInfos.size(); i++) {
        rankProfileManagerPtr->addRankProfile(
                new RankProfile(rankProfileInfos[i]));
    }
    if (!rankProfileManagerPtr->init(_resourceReaderPtr, _cavaPluginManagerPtr, _metricsReporter)) {
        HA3_LOG(ERROR, "RankProfileManager init failed!");
        return RankProfileManagerPtr();
    }
    return rankProfileManagerPtr;
}

END_HA3_NAMESPACE(rank);
