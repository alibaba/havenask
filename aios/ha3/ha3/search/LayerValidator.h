#ifndef ISEARCH_LAYERVALIDATOR_H
#define ISEARCH_LAYERVALIDATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryLayerClause.h>
#include <ha3/common/RankHint.h>
#include <indexlib/index_base/index_meta/partition_meta.h>

BEGIN_HA3_NAMESPACE(search);

class LayerValidator
{
public:
    LayerValidator();
    ~LayerValidator();
private:
    LayerValidator(const LayerValidator &);
    LayerValidator& operator=(const LayerValidator &);
public:
    bool validateRankHint(const IE_NAMESPACE(index_base)::PartitionMetaPtr& meta,
                          const common::RankHint* hint, 
                          const common::LayerKeyRange* keyRange,
                          size_t dimenCount);
private:
    bool validateLayerWithoutDimen(
            const IE_NAMESPACE(index_base)::PartitionMetaPtr& meta,
            const common::RankHint* hint);
    bool compareRankHint(
            const IE_NAMESPACE(index_base)::SortDescription& sortDesc,
            const common::RankHint* hint);

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LayerValidator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_LAYERVALIDATOR_H
