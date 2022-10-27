#ifndef ISEARCH_QUERYEXECUTORRESTRICTOR_H
#define ISEARCH_QUERYEXECUTORRESTRICTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/common/TimeoutTerminator.h>

BEGIN_HA3_NAMESPACE(search);

class LayerMetaWrapper {
public:
    LayerMetaWrapper(const LayerMeta *layerMeta);
    ~LayerMetaWrapper();
private:
    LayerMetaWrapper(const LayerMetaWrapper&);
    LayerMetaWrapper& operator= (const LayerMetaWrapper &);
public:
    docid_t seek(docid_t docId);
    void reset();
private:
    const LayerMeta *_layerMeta;
    uint32_t _offset;
};

class QueryExecutorRestrictor
{
public:
    QueryExecutorRestrictor();
    ~QueryExecutorRestrictor();
private:
    QueryExecutorRestrictor(const QueryExecutorRestrictor &);
    QueryExecutorRestrictor& operator=(const QueryExecutorRestrictor &);
public:
    void setTimeoutTerminator(common::TimeoutTerminator *timeoutTerminator) {
        _timeoutTerminator = timeoutTerminator;
    }
    void setLayerMeta(const LayerMeta *layerMeta) {
        DELETE_AND_SET_NULL(_layerMetaWrapper);
        if (layerMeta != NULL) {
            _layerMetaWrapper = new LayerMetaWrapper(layerMeta);
        }
    }
    
    docid_t meetRestrict(docid_t curDocId);
    void reset();
private:
    common::TimeoutTerminator *_timeoutTerminator;
    LayerMetaWrapper *_layerMetaWrapper;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QueryExecutorRestrictor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_QUERYEXECUTORRESTRICTOR_H
