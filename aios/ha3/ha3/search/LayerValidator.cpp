#include <algorithm> 
#include <ha3/search/LayerValidator.h>

IE_NAMESPACE_USE(index_base);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, LayerValidator);

LayerValidator::LayerValidator() { 
}

LayerValidator::~LayerValidator() { 
}

bool LayerValidator::compareRankHint(
        const IE_NAMESPACE(index_base)::SortDescription& sortDesc,
        const RankHint* hint)
{
    if (hint->sortField == sortDesc.sortFieldName 
        && hint->sortPattern == sortDesc.sortPattern)
    {
        return true;
    } else {
        return false;
    }
}

bool LayerValidator::validateLayerWithoutDimen(
        const PartitionMetaPtr& meta,
        const RankHint* hint)
{
    if (meta->Size() == 0) {
        return false;
    } 
    return compareRankHint(meta->GetSortDescription(0), hint);
}

bool LayerValidator::validateRankHint(
        const PartitionMetaPtr& meta,
        const RankHint* hint, 
        const LayerKeyRange *keyRange,
        size_t dimenCount)
{
    if (!hint || hint->sortField.empty()) {
        return true;
    }
    if (dimenCount == 0) {
        return validateLayerWithoutDimen(meta, hint);
    }
    
    size_t partMetaDimenCount = meta->Size();
    if (dimenCount > partMetaDimenCount) {
        HA3_LOG(DEBUG, "set QM_PER_LAYER because of dimenCount exceed partMetaCount");
        return false;
    }
    if (keyRange->ranges.empty() && dimenCount < partMetaDimenCount) {
        return compareRankHint(meta->GetSortDescription(dimenCount), hint);
    } else if (keyRange->values.empty() && keyRange->ranges.size() == 1) {
        return compareRankHint(meta->GetSortDescription(dimenCount - 1), hint);
    } else {
        return false;
    }
}

END_HA3_NAMESPACE(search);

