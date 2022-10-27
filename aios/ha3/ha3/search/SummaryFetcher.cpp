#include <ha3/search/SummaryFetcher.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SummaryFetcher);

bool SummaryFetcher::fillAttributeToSummary(docid_t docid, 
        IE_NAMESPACE(document)::SearchSummaryDocument *summaryDoc)
{
    assert(_matchDocAllocator);
    if (_attributes.size() == 0) {
        return true;
    }
    auto matchDoc = _matchDocAllocator->allocate(docid);
    map<summaryfieldid_t, AttributeExpression*>::iterator it = 
        _attributes.begin();
    for (; it != _attributes.end(); ++it) {
        AttributeExpression *attrExpr = it->second;
        attrExpr->evaluate(matchDoc);
        auto reference = attrExpr->getReferenceBase();
        string str = reference->toString(matchDoc);
        summaryDoc->SetFieldValue(it->first, str.c_str(), str.size());
    }
    _matchDocAllocator->deallocate(matchDoc);
    return true;
}

END_HA3_NAMESPACE(search);

