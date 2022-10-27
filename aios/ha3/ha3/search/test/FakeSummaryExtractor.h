#ifndef ISEARCH_FAKESUMMARYEXTRACTOR_H
#define ISEARCH_FAKESUMMARYEXTRACTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryExtractor.h>

BEGIN_HA3_NAMESPACE(search);

class FakeSummaryExtractor : public summary::SummaryExtractor
{
public:
    FakeSummaryExtractor();
    ~FakeSummaryExtractor();
public:
    /* override */ bool beginRequest(summary::SummaryExtractorProvider *provider);
    /* override */ void endRequest();
    /* override */ void extractSummary(common::SummaryHit &summaryHit);
    /* override */ summary::SummaryExtractor* clone();
    /* override */ void destory();
public:
    void setFilledAttributes(const std::vector<std::string> attributes) {
        _attributes = attributes;
    }
    void setBeginRequestSuccessFlag(bool isBeginRequestSuccess) {
        _isBeginRequestSuccess = isBeginRequestSuccess;
    }
private:
    std::vector<std::string> _attributes;
    std::vector<char*> _values;
    bool _isBeginRequestSuccess;
    char *_p;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeSummaryExtractor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKESUMMARYEXTRACTOR_H
