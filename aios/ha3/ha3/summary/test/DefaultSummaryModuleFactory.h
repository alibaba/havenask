#ifndef ISEARCH_DEFAULTSUMMARYMODULEFACTORY_H
#define ISEARCH_DEFAULTSUMMARYMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryModuleFactory.h>
#include <ha3/summary/DefaultSummaryExtractor.h>

BEGIN_HA3_NAMESPACE(summary);

class DefaultSummaryModuleFactory : public SummaryModuleFactory
{
public:
    DefaultSummaryModuleFactory();
    virtual ~DefaultSummaryModuleFactory();
public:
    virtual bool init(const KeyValueMap &parameters) {
        return true;
    }
    virtual void destroy() {
        delete this;
    }
    virtual SummaryExtractor* createSummaryExtractor(const char *extractorName, 
            const KeyValueMap &parameters, config::ResourceReader *resourceReader,
            config::HitSummarySchema *hitSummarySchema) 
    {
        return new DefaultSummaryExtractor();
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(DefaultSummaryModuleFactory);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_DEFAULTSUMMARYMODULEFACTORY_H
