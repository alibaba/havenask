#ifndef ISEARCH_CAVASUMMARYEXTRACTOR_H
#define ISEARCH_CAVASUMMARYEXTRACTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>
#include <ha3/summary/SummaryExtractor.h>
#include <suez/turing/expression/cava/common/CavaSummaryModuleInfo.h>
#include <suez/turing/expression/cava/impl/TraceApi.h>

BEGIN_HA3_NAMESPACE(summary);

class CavaSummaryExtractor : public SummaryExtractor
{
public:
    CavaSummaryExtractor(const KeyValueMap &parameters,
                         const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr,
                         kmonitor::MetricsReporter *metricsReporter);
    ~CavaSummaryExtractor();
public:
    bool beginRequest(SummaryExtractorProvider *provider) override;
    void extractSummary(common::SummaryHit &summaryHit) override;
    CavaSummaryExtractor* clone() override {
       return new CavaSummaryExtractor(*this);
    }
    void destory() override { delete this; }

private:
    suez::turing::CavaPluginManagerPtr _cavaPluginManager;
    kmonitor::MetricsReporter *_metricsReporter;
    std::string _defaultClassName;
    std::string _summaryNameKey;
    common::Tracer *_tracer;
    void *_summaryObj;
    CavaCtx *_cavaCtx;
    suez::turing::CavaModuleInfoPtr _cavaModuleInfo;
    suez::turing::CavaSummaryModuleInfo *_summaryModuleInfo;

private:
    friend class CavaSummaryExtractorTest;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(CavaSummaryExtractor);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_CAVASUMMARYEXTRACTOR_H
