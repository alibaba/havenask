#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryModuleFactory.h>
#include <summary_demo/HighlightNumberExtractor.h>

USE_HA3_NAMESPACE(summary);
namespace pluginplatform {
namespace summary_plugins {

class SummaryModuleFactorySample : public SummaryModuleFactory
{
public:
    SummaryModuleFactorySample();
    virtual ~SummaryModuleFactorySample();
public:
    virtual bool init(const KeyValueMap &parameters) {
        return true;
    }
    virtual void destroy() {
        delete this;
    }
    virtual SummaryExtractor* createSummaryExtractor(const char *extractorName, 
            const KeyValueMap &parameters, isearch::config::ResourceReader *resourceReader,
            isearch::config::HitSummarySchema *hitSummarySchema) 
    {
        if (extractorName == std::string("HighlightNumberExtractor")) {
            std::vector<std::string> attrNames;
            attrNames.push_back(std::string("price"));
            return new HighlightNumberExtractor(attrNames);
        }
        return NULL; // return NULL when error occurs
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryModuleFactorySample);

}}
