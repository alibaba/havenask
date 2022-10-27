#ifndef ISEARCH_SUMMARYPROFILE_H
#define ISEARCH_SUMMARYPROFILE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/SummaryProfileInfo.h>
#include <ha3/summary/SummaryExtractorChain.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/config/ResourceReader.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/HitSummarySchema.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>
#include <ha3/summary/CavaSummaryExtractor.h>

BEGIN_HA3_NAMESPACE(summary);

class SummaryProfile
{
public:
    SummaryProfile(const config::SummaryProfileInfo &summaryProfileInfo,
                   config::HitSummarySchema *hitSummarySchema);
    ~SummaryProfile();
public:
    bool init(build_service::plugin::PlugInManagerPtr &plugInManagerPtr,
              config::ResourceReaderPtr &resourceReaderPtr,
              const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr,
              kmonitor::MetricsReporter *metricsReporter);
    SummaryExtractorChain* createSummaryExtractorChain() const;
    std::string getProfileName() const;
    const config::FieldSummaryConfigVec& getFieldSummaryConfig() const {
        return _summaryProfileInfo._fieldSummaryConfigVec;
    }
    const config::SummaryProfileInfo& getSummaryProfileInfo() const {
        return _summaryProfileInfo;
    }
private:
    SummaryExtractor* createSummaryExtractor(
        build_service::plugin::PlugInManagerPtr &plugInManagerPtr,
        config::ResourceReaderPtr &resourceReaderPtr,
        const config::ExtractorInfo &extractorInfo,
        const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr,
        kmonitor::MetricsReporter *metricsReporter
        );
public:
    //for test
    void resetSummaryExtractor(SummaryExtractor *summaryExtractor) {
        if (_summaryExtractorChain != NULL) {
            _summaryExtractorChain->destroy();
            _summaryExtractorChain = NULL;
        }
        _summaryExtractorChain = new SummaryExtractorChain;
        _summaryExtractorChain->addSummaryExtractor(summaryExtractor);
    }
private:
    config::SummaryProfileInfo _summaryProfileInfo;
    SummaryExtractorChain *_summaryExtractorChain;
    config::HitSummarySchema *_hitSummarySchema;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryProfile);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_SUMMARYPROFILE_H
