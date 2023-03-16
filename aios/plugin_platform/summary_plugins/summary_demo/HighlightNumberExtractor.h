#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryExtractor.h>

USE_HA3_NAMESPACE(summary);
namespace pluginplatform {
namespace summary_plugins {

class HighlightNumberExtractor : public SummaryExtractor
{
public:
    HighlightNumberExtractor(const std::vector<std::string> &toSummaryAttributes);
    HighlightNumberExtractor(const HighlightNumberExtractor &);
    ~HighlightNumberExtractor();
private:
    HighlightNumberExtractor& operator=(const HighlightNumberExtractor &);
public:
    /* override */ void extractSummary(isearch::common::SummaryHit &summaryHit);
    /* override */ bool beginRequest(SummaryExtractorProvider *provider);
    /* override */ SummaryExtractor* clone();
    /* override */ void destory();
    /* override */ void endRequest() {}
private:
    void highlightNumber(const std::string &input, std::string &output, 
                         const isearch::config::FieldSummaryConfig *configPtr);
private:
    std::vector<std::string> _attrNames;
    const isearch::config::FieldSummaryConfigVec *_configVec;
    std::set<std::string> _keywords;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(HighlightNumberExtractor);

}}
