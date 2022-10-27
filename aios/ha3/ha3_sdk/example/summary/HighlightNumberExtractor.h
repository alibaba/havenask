#ifndef ISEARCH_HIGHLIGHTNUMBEREXTRACTOR_H
#define ISEARCH_HIGHLIGHTNUMBEREXTRACTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryExtractor.h>

BEGIN_HA3_NAMESPACE(summary);

class HighlightNumberExtractor : public SummaryExtractor
{
public:
    HighlightNumberExtractor(const std::vector<std::string> &toSummaryAttributes);
    HighlightNumberExtractor(const HighlightNumberExtractor &);
    ~HighlightNumberExtractor();
private:
    HighlightNumberExtractor& operator=(const HighlightNumberExtractor &);
public:
    /* override */ void extractSummary(common::SummaryHit &summaryHit);
    /* override */ bool beginRequest(SummaryExtractorProvider *provider);
    /* override */ SummaryExtractor* clone();
    /* override */ void destory();
    /* override */ void endRequest() {}
private:
    void highlightNumber(const std::string &input, std::string &output, 
                         const config::FieldSummaryConfig *configPtr);
private:
    std::vector<std::string> _attrNames;
    const config::FieldSummaryConfigVec *_configVec;
    std::set<std::string> _keywords;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(HighlightNumberExtractor);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_HIGHLIGHTNUMBEREXTRACTOR_H
