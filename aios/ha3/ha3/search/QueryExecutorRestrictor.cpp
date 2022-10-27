#include <ha3/search/QueryExecutorRestrictor.h>

using namespace std;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, QueryExecutorRestrictor);

LayerMetaWrapper::LayerMetaWrapper(const LayerMeta *layerMeta)
 : _layerMeta(layerMeta)
{
    reset();
}

docid_t LayerMetaWrapper::seek(docid_t curDocId) {
    while (_offset < _layerMeta->size() && 
           (*_layerMeta)[_offset].end < curDocId)
    {
        ++_offset;
    }
    if (_offset < _layerMeta->size()) {
        return max(curDocId, (*_layerMeta)[_offset].begin);
    } else {
        return END_DOCID;
    }
}

void LayerMetaWrapper::reset() {
    _offset = 0;
}

LayerMetaWrapper::~LayerMetaWrapper () {
}

QueryExecutorRestrictor::QueryExecutorRestrictor() 
    : _timeoutTerminator(NULL)
    , _layerMetaWrapper(NULL)
{ 
}

QueryExecutorRestrictor::~QueryExecutorRestrictor() { 
    DELETE_AND_SET_NULL(_layerMetaWrapper);
}

docid_t QueryExecutorRestrictor::meetRestrict(docid_t curDocId) {
    if (_timeoutTerminator && _timeoutTerminator->checkRestrictorTimeout()) {
        return END_DOCID;
    }
    if (_layerMetaWrapper) {
        return _layerMetaWrapper->seek(curDocId);
    }
    return curDocId;
}

void QueryExecutorRestrictor::reset() {
    if (_layerMetaWrapper) {
        _layerMetaWrapper->reset();
    }
}
END_HA3_NAMESPACE(search);

