#include <ha3/sql/ops/scan/RangeQueryExecutor.h>

using namespace std;
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(sql);

RangeQueryExecutor::RangeQueryExecutor(LayerMeta* layerMeta)
    : _layerMeta(*layerMeta)
    , _rangeIdx(0)
{
    _df = 0;
    _rangeCount = _layerMeta.size();
    for (auto &rangeMeta : _layerMeta) {
        _df +=  rangeMeta.end - rangeMeta.begin + 1;
    }

}

RangeQueryExecutor::~RangeQueryExecutor() {
}

void RangeQueryExecutor::reset() {
    _rangeIdx = 0;
}

IE_NAMESPACE(common)::ErrorCode RangeQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result) {
    if (getDocId() == docId && subDocId < subDocEnd) {
        result = subDocId;
    } else {
        result = END_DOCID;
    }
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

IE_NAMESPACE(common)::ErrorCode RangeQueryExecutor::doSeek(docid_t id, docid_t& result) {
    if (unlikely(id == END_DOCID)) {
        result = END_DOCID;
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
    ++_seekDocCount;
    for (; _rangeIdx < _rangeCount; ++_rangeIdx) {
        auto &range = _layerMeta[_rangeIdx];
        if (likely(id <= range.end)) {
            result =  id >= range.begin ? id : range.begin;
            return IE_NAMESPACE(common)::ErrorCode::OK;
        }
    }
    result = END_DOCID;
    return IE_NAMESPACE(common)::ErrorCode::OK;

}

std::string RangeQueryExecutor::toString() const {
    return _layerMeta.toString();
}

bool RangeQueryExecutor::isMainDocHit(docid_t docId) const {
    return true;
}

END_HA3_NAMESPACE(sql);
