#ifndef ISEARCH_LAYERRANGEDISTRIBUTOR_H
#define ISEARCH_LAYERRANGEDISTRIBUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerMetas.h>
#include <autil/mem_pool/PoolVector.h>
#include <ha3/common/Tracer.h>

BEGIN_HA3_NAMESPACE(search);

class LayerRangeDistributor
{
public:
    LayerRangeDistributor(LayerMetas *layerMetas,
                          autil::mem_pool::Pool *pool,
                          uint32_t docCount,
                          uint32_t rankSize,
                          common::Tracer *tracer = NULL);
    ~LayerRangeDistributor();
private:
    LayerRangeDistributor(const LayerRangeDistributor &);
    LayerRangeDistributor& operator=(const LayerRangeDistributor &);
public:
    void moveToNextLayer(uint32_t leftQuota);
    bool hasNextLayer() const {
        return _curLayerCursor < (int32_t)_layerMetas->size();
    }
    
    LayerMeta *getCurLayer(uint32_t &layer) {
        layer = _curLayerCursor;
        return &_curLayer;
    }
private:
    static void initLayerQuota(LayerMetas *layerMetas,uint32_t rankSize);
    static void proportionalLayerQuota(LayerMeta &layerMeta);
    static void averageLayerQuota(LayerMeta &layerMeta);
    static LayerMeta reverseRange(autil::mem_pool::Pool *pool,
                                  const LayerMeta &curLayer,
                                  uint32_t totalDocCount);
    static LayerMeta updateLayerMetaForNextLayer(autil::mem_pool::Pool *pool,
            const LayerMeta &curLayer, LayerMeta &nextLayer,
            uint32_t &leftQuota, uint32_t totalDocCount);
    static bool hasQuota(const LayerMeta &layerMeta);
private:
    LayerMetas *_layerMetas;
    autil::mem_pool::Pool *_pool;
    LayerMeta _seekedRange;
    LayerMeta _curLayer;
    uint32_t _docCount;
    uint32_t _leftQuota;
    int32_t _curLayerCursor;
    common::Tracer *_tracer;
private:
    friend class LayerRangeDistributorTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LayerRangeDistributor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_LAYERRANGEDISTRIBUTOR_H
