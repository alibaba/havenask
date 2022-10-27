#include <ha3/sql/ops/scan/Ha3ScanIterator.h>

BEGIN_HA3_NAMESPACE(sql);

Ha3ScanIterator::Ha3ScanIterator(const Ha3ScanIteratorParam &param)
    : ScanIterator(param.matchDocAllocator, param.timeoutTerminator) {
    _queryExecutor = param.queryExecutor;
    if (param.layerMeta) {
        _layerMeta.reset(new search::LayerMeta(*param.layerMeta));
    }
    _filterWrapper = param.filterWrapper;
    _delMapReader = param.delMapReader;
    _subDelMapReader = param.subDelMapReader;
    _needSubDoc = param.matchDocAllocator->hasSubDocAllocator();
    _singleLayerSearcher.reset(new search::SingleLayerSearcher(
                    _queryExecutor.get(), _layerMeta.get(), _filterWrapper.get(), _delMapReader.get(),
                    _matchDocAllocator.get(), param.timeoutTerminator,
                    param.mainToSubIt, _subDelMapReader.get(), param.matchDataManager,
                    param.needAllSubDocFlag));
    _eof = false;
}

Ha3ScanIterator::~Ha3ScanIterator() {
}

uint32_t Ha3ScanIterator::getTotalScanCount() {
    return _singleLayerSearcher->getSeekTimes();
}

bool Ha3ScanIterator::batchSeek(size_t batchSize, std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (_eof) {
        return true;
    }
    if (batchSize > 0) {
        matchdoc::MatchDoc doc;
        IE_NAMESPACE(common)::ErrorCode ec;
        for (size_t i = 0; i < batchSize; ++i) {
            ec = _singleLayerSearcher->seek(_needSubDoc, doc);
            if (ec != IE_NAMESPACE(common)::ErrorCode::OK) {
                return false;
            }
            if (matchdoc::INVALID_MATCHDOC == doc) {
                _eof = true;
                return true;
            }
            matchDocs.push_back(doc);
        }
        return false;
    } else {
        matchdoc::MatchDoc doc;
        IE_NAMESPACE(common)::ErrorCode ec;
        while (true) {
            ec = _singleLayerSearcher->seek(_needSubDoc, doc);
            if (ec != IE_NAMESPACE(common)::ErrorCode::OK) {
                return false;
            }
            if (matchdoc::INVALID_MATCHDOC == doc) {
                _eof = true;
                return true;
            }
            matchDocs.push_back(doc);
        }
    }
}

END_HA3_NAMESPACE(sql);
