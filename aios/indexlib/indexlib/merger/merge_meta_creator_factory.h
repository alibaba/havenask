#ifndef __INDEXLIB_MERGE_META_CREATOR_FACTORY_H
#define __INDEXLIB_MERGE_META_CREATOR_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/merge_meta_creator.h"

DECLARE_REFERENCE_CLASS(index, ReclaimMapCreator);
DECLARE_REFERENCE_CLASS(merger, SplitSegmentStrategyFactory);

IE_NAMESPACE_BEGIN(merger);

class MergeMetaCreatorFactory
{
public:
    MergeMetaCreatorFactory();
    ~MergeMetaCreatorFactory();
public:
    static MergeMetaCreatorPtr Create(
            config::IndexPartitionSchemaPtr& schema,
            const index::ReclaimMapCreatorPtr& reclaimMapCreator,
            SplitSegmentStrategyFactory* splitStrategyFactory);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeMetaCreatorFactory);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_META_CREATOR_FACTORY_H
