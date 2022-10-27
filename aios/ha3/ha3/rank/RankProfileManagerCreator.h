
#ifndef ISEARCH_RANKPROFILEMANAGERCREATOR_H
#define ISEARCH_RANKPROFILEMANAGERCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/config/RankProfileConfig.h>

BEGIN_HA3_NAMESPACE(rank);

class RankProfileManagerCreator
{
public:
    RankProfileManagerCreator(const config::ResourceReaderPtr &resourceReaderPtr,
                              const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr,
                              kmonitor::MetricsReporter *metricsReporter);
    ~RankProfileManagerCreator();
public:
    RankProfileManagerPtr create(const config::RankProfileConfig &config);
public:
    // for test
    RankProfileManagerPtr createFromFile(const std::string &filePath);
    RankProfileManagerPtr createFromString(const std::string &configStr);
private:
    void addRecordProfileInfo(config::RankProfileInfos& rankProfileInfos);
private:
    config::ResourceReaderPtr _resourceReaderPtr;
    suez::turing::CavaPluginManagerPtr _cavaPluginManagerPtr;
    kmonitor::MetricsReporter *_metricsReporter;
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_RANKPROFILEMANAGERCREATOR_H
