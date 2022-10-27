#ifndef ISEARCH_SUMMARYMODULEFACTORY_H
#define ISEARCH_SUMMARYMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/ModuleFactory.h>
#include <ha3/summary/SummaryExtractor.h>
#include <ha3/config/ResourceReader.h>

BEGIN_HA3_NAMESPACE(summary);

static const std::string MODULE_FUNC_SUMMARY = "_Summary";
class SummaryModuleFactory : public util::ModuleFactory
{
public:
    SummaryModuleFactory() {}
    virtual ~SummaryModuleFactory() {}
public:
    virtual SummaryExtractor* createSummaryExtractor(const char *extractorName, 
            const KeyValueMap &extractorParameters,
            config::ResourceReader *resourceReader,
            config::HitSummarySchema *hitSummarySchema) = 0;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryModuleFactory);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_SUMMARYMODULEFACTORY_H
