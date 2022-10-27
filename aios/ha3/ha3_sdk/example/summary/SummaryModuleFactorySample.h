#ifndef ISEARCH_SUMMARYMODULEFACTORYSAMPLE_H
#define ISEARCH_SUMMARYMODULEFACTORYSAMPLE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryModuleFactory.h>
#include <ha3_sdk/example/summary/SummaryExtractorSample.h>
#include <ha3_sdk/example/summary/HighlightNumberExtractor.h>

BEGIN_HA3_NAMESPACE(summary);

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
            const KeyValueMap &parameters, config::ResourceReader *resourceReader,
            config::HitSummarySchema *hitSummarySchema) 
    {
        if (extractorName == std::string("SummaryExtractorSample")) {
            summaryfieldid_t newlyAddedFieldId = 
                hitSummarySchema->declareSummaryField("new_field_name");
            if (newlyAddedFieldId == INVALID_SUMMARYFIELDID) {
                return NULL; // field name already in use, handle error or use the exists field.
            }
            return new SummaryExtractorSample(newlyAddedFieldId);
        } else if (extractorName == std::string("HighlightNumberExtractor")) {
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

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_SUMMARYMODULEFACTORYSAMPLE_H
