#include "indexlib/merger/merge_meta_creator_factory.h"
#include "indexlib/merger/index_table_merge_meta_creator.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map_creator.h"

using namespace std;

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergeMetaCreatorFactory);

MergeMetaCreatorFactory::MergeMetaCreatorFactory()
{
}

MergeMetaCreatorFactory::~MergeMetaCreatorFactory()
{
}

MergeMetaCreatorPtr MergeMetaCreatorFactory::Create(
        config::IndexPartitionSchemaPtr& schema,
        const index::ReclaimMapCreatorPtr& reclaimMapCreator,
        SplitSegmentStrategyFactory* splitStrategyFactory)
{
    switch(schema->GetTableType())
    {
    case tt_index:
        return MergeMetaCreatorPtr(
            new IndexTableMergeMetaCreator(schema, reclaimMapCreator, splitStrategyFactory));
    default:
        assert(false);
    }
    return MergeMetaCreatorPtr();
}


IE_NAMESPACE_END(merger);
