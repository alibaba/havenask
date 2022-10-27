#ifndef ISEARCH_SUMMARYFETCHER_H
#define ISEARCH_SUMMARYFETCHER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/summary/summary_reader.h>
#include <ha3/common/Hit.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/isearch.h>

BEGIN_HA3_NAMESPACE(search);

class SummaryFetcher
{
public:
    SummaryFetcher(const std::map<summaryfieldid_t, suez::turing::AttributeExpression*>
                   &attributes,
                   matchdoc::MatchDocAllocator *matchDocAllocator,
                   IE_NAMESPACE(index)::SummaryReaderPtr summaryReader)
        : _attributes(attributes)
        , _matchDocAllocator(matchDocAllocator)
        , _summaryReader(summaryReader)
    {
    }
    virtual ~SummaryFetcher() {_attributes.clear();}
private:
    SummaryFetcher(const SummaryFetcher &);
    SummaryFetcher& operator=(const SummaryFetcher &);
public:
    bool fetchSummary(common::Hit *hit, const SummaryGroupIdVec &summaryGroupIdVec)
    {
        IE_NAMESPACE(document)::SearchSummaryDocument *summaryDoc =
            hit->getSummaryHit()->getSummaryDocument();
        docid_t docId = hit->getDocId();
        if (docId < 0) {
            return false;
        }
        if (!_summaryReader) {
            return fillAttributeToSummary(docId, summaryDoc);
        }
        if (summaryGroupIdVec.empty()) {
            return _summaryReader->GetDocument(docId, summaryDoc)
                && fillAttributeToSummary(docId, summaryDoc);
        }
        else {
            return _summaryReader->GetDocument(docId, summaryDoc, summaryGroupIdVec)
                && fillAttributeToSummary(docId, summaryDoc);
        }
    }
    bool fillAttributeToSummary(docid_t docid,
                                IE_NAMESPACE(document)::SearchSummaryDocument *summaryDoc);
private:
    std::map<summaryfieldid_t, suez::turing::AttributeExpression*> _attributes;
    matchdoc::MatchDocAllocator *_matchDocAllocator;
    IE_NAMESPACE(index)::SummaryReaderPtr _summaryReader;
protected:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryFetcher);
END_HA3_NAMESPACE(search);

#endif //ISEARCH_SUMMARYFETCHER_H
