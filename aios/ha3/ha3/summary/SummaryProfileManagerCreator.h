#ifndef ISEARCH_SUMMARYPROFILEMANAGERCREATOR_H
#define ISEARCH_SUMMARYPROFILEMANAGERCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryProfileManager.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/SummaryProfileConfig.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>


BEGIN_HA3_NAMESPACE(summary);

class SummaryProfileManagerCreator
{
public:
    SummaryProfileManagerCreator(const config::ResourceReaderPtr &resourceReaderPtr,
                                 const config::HitSummarySchemaPtr &hitSummarySchema,
                                 const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr
                                 = suez::turing::CavaPluginManagerPtr(),
                                 kmonitor::MetricsReporter *metricsReporter = NULL
                                 )
        : _resourceReaderPtr(resourceReaderPtr)
        , _hitSummarySchema(hitSummarySchema)
        , _cavaPluginManagerPtr(cavaPluginManagerPtr)
        , _metricsReporter(metricsReporter)
    {
    }
    ~SummaryProfileManagerCreator() {}
public:
    SummaryProfileManagerPtr create(const config::SummaryProfileConfig &config);
    SummaryProfileManagerPtr createFromString(const std::string &configStr);
private:
    config::ResourceReaderPtr _resourceReaderPtr;
    config::HitSummarySchemaPtr _hitSummarySchema;
    suez::turing::CavaPluginManagerPtr _cavaPluginManagerPtr;
    kmonitor::MetricsReporter *_metricsReporter;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryProfileManagerCreator);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_SUMMARYPROFILEMANAGERCREATOR_H
