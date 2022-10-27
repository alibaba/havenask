#ifndef ISEARCH_SUMMARYEXTRACTORCHAIN_H
#define ISEARCH_SUMMARYEXTRACTORCHAIN_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryExtractor.h>

BEGIN_HA3_NAMESPACE(summary);

class SummaryExtractorChain
{
public:
    typedef std::vector<SummaryExtractor *> SummaryExtractorList;
    typedef SummaryExtractorList::iterator Iterator;
    typedef SummaryExtractorList::const_iterator ConstIterator;
public:
    SummaryExtractorChain();
    SummaryExtractorChain(const SummaryExtractorChain &other);
    ~SummaryExtractorChain();
private:
    SummaryExtractorChain& operator=(const SummaryExtractorChain &);
public:
    void addSummaryExtractor(SummaryExtractor *extractor);
    size_t getSummaryExtractorCount() const { return _extractors.size(); }

    // wrappers for SummaryExtractor
    bool beginRequest(SummaryExtractorProvider *provider);
    void extractSummary(common::SummaryHit &summaryHit);
    void endRequest();

    SummaryExtractorChain* clone() const;
    void destroy();
private:
    SummaryExtractorList _extractors;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryExtractorChain);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_SUMMARYEXTRACTORCHAIN_H
