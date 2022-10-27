#ifndef ISEARCH_LAYERMETAUTIL_H
#define ISEARCH_LAYERMETAUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/common/RankClause.h>
#include <ha3/common/QueryLayerClause.h>

BEGIN_HA3_NAMESPACE(search);

class LayerMetaUtil
{
public:
    LayerMetaUtil();
    ~LayerMetaUtil();
private:
    LayerMetaUtil(const LayerMetaUtil &);
    LayerMetaUtil& operator=(const LayerMetaUtil &);
public:
    static LayerMeta reverseRange(autil::mem_pool::Pool *pool,
            const LayerMeta &curLayer, uint32_t totalDocCount);
    static LayerMeta reverseRangeWithEnd(autil::mem_pool::Pool *pool,
            const LayerMeta &curLayer, uint32_t totalDocCount);
    static LayerMeta accumulateRanges(autil::mem_pool::Pool *pool, 
            const LayerMeta& layerMetaA,
            const LayerMeta& layerMetaB);
    static bool splitLayerMetas(autil::mem_pool::Pool *pool,
                               const LayerMetas &layerMetas,
                               const IndexPartitionReaderWrapperPtr &readerWrapper,
                               size_t totalWayCount,
                               std::vector<LayerMetasPtr> &splittedLayerMetas);
    static LayerMetasPtr createLayerMetas(
            const HA3_NS(common)::QueryLayerClause *queryLayerClause,
            const HA3_NS(common)::RankClause *rankClause,
            IndexPartitionReaderWrapper *readerWrapper,
            autil::mem_pool::Pool *pool);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LayerMetaUtil);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_LAYERMETAUTIL_H
