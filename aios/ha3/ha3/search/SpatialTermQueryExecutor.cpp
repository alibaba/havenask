#include <ha3/search/SpatialTermQueryExecutor.h>
#include <indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h>

using namespace std;
IE_NAMESPACE_USE(index);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SpatialTermQueryExecutor);

SpatialTermQueryExecutor::SpatialTermQueryExecutor(
        IE_NAMESPACE(index)::PostingIterator *iter, 
        const common::Term &term)
    : TermQueryExecutor(iter, term)
    , _filter(NULL)
    , _testDocCount(0)
{
    initSpatialPostingIterator();
}

SpatialTermQueryExecutor::~SpatialTermQueryExecutor() { 
}

IE_NAMESPACE(common)::ErrorCode SpatialTermQueryExecutor::doSeek (
        docid_t id, docid_t& result)
{
    docid_t tempDocId = INVALID_DOCID;
    auto ec = _spatialIter->InnerSeekDoc(id, tempDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    if (_filter) {
        while (tempDocId != INVALID_DOCID) {
            if (_filter->Test(tempDocId)) {
                _testDocCount++;
                break;
            }
            auto ec = _spatialIter->InnerSeekDoc(tempDocId, tempDocId);
            IE_RETURN_CODE_IF_ERROR(ec);
        }
    }
    if (tempDocId == INVALID_DOCID) {
        tempDocId = END_DOCID;
    }
    result = tempDocId;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

void SpatialTermQueryExecutor::initSpatialPostingIterator() {
    SeekAndFilterIterator* compositeIter = (SeekAndFilterIterator*)_iter;
    _spatialIter = dynamic_cast<IE_NAMESPACE(index)::SpatialPostingIterator*>(
            compositeIter->GetIndexIterator());
    _filter = compositeIter->GetDocValueFilter();
}

END_HA3_NAMESPACE(search);

