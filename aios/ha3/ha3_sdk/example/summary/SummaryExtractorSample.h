#ifndef ISEARCH_SUMMARYEXTRACTORSAMPLE_H
#define ISEARCH_SUMMARYEXTRACTORSAMPLE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryExtractor.h>

BEGIN_HA3_NAMESPACE(summary);

class SummaryExtractorSample : public SummaryExtractor
{
public:
    SummaryExtractorSample(summaryfieldid_t newlyAddedFieldId);
    ~SummaryExtractorSample();
public:
    /* override */ void extractSummary(common::SummaryHit &summaryHit);
    /* override */ bool beginRequest(SummaryExtractorProvider *provider);
    /* override */ SummaryExtractor* clone();
    /* override */ void destory();
    /* override */ void endRequest() {}
public:
    void extractSingleSummary(const std::string &fieldName,
                              const std::string &content, std::string &output);
    std::string deleteSpaceCharacter(const std::string &str);
    std::string getSummary(const std::string &text, 
                           const std::vector<std::string>& keywords,
                           const config::FieldSummaryConfig *configPtr);
    std::string highlight(const std::string &summary, std::map<size_t,size_t> &posMap,
                          const std::string &highlightPrefix, const std::string &highlightSuffix);
    std::string getAdjustedText(const std::string &text, size_t summaryLen);
private:
    typedef std::pair<size_t, int> PosInfo;
    const config::FieldSummaryConfigVec *_configVec;
    std::string _queryString;
    std::vector<std::string> _keywords;
    summaryfieldid_t _newlyAddedFieldId;
    HA3_LOG_DECLARE();

private:
    friend class SummaryExtractorSampleTest;
};

HA3_TYPEDEF_PTR(SummaryExtractorSample);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_SUMMARYEXTRACTORSAMPLE_H
