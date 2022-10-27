#ifndef ISEARCH_SUMMARYEXTRACTOR_H
#define ISEARCH_SUMMARYEXTRACTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Term.h>
#include <ha3/summary/SummaryExtractorProvider.h>
#include <ha3/common/SummaryHit.h>

BEGIN_HA3_NAMESPACE(summary);

// for plugin compatibility
typedef config::FieldSummaryConfig FieldSummaryConfig;
typedef config::FieldSummaryConfigVec FieldSummaryConfigVec;
typedef config::FieldSummaryConfigMap FieldSummaryConfigMap;

class SummaryExtractor
{
public:
    SummaryExtractor() {}
    virtual ~SummaryExtractor() {}
public:
    virtual void extractSummary(common::SummaryHit &summaryHit) = 0;

    virtual bool beginRequest(SummaryExtractorProvider *provider) { return true; }
    virtual void endRequest() {}

    virtual SummaryExtractor* clone() = 0;
    virtual void destory() = 0;
};

HA3_TYPEDEF_PTR(SummaryExtractor);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_SUMMARYEXTRACTOR_H
