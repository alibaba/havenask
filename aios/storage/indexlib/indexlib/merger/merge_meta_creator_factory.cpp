/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/merger/merge_meta_creator_factory.h"

#include "indexlib/index/merger_util/reclaim_map/reclaim_map_creator.h"
#include "indexlib/merger/index_table_merge_meta_creator.h"
#include "indexlib/merger/key_value_merge_meta_creator.h"

using namespace std;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeMetaCreatorFactory);

MergeMetaCreatorFactory::MergeMetaCreatorFactory() {}

MergeMetaCreatorFactory::~MergeMetaCreatorFactory() {}

MergeMetaCreatorPtr MergeMetaCreatorFactory::Create(config::IndexPartitionSchemaPtr& schema,
                                                    const index::ReclaimMapCreatorPtr& reclaimMapCreator,
                                                    SplitSegmentStrategyFactory* splitStrategyFactory)
{
    switch (schema->GetTableType()) {
    case tt_index:
        return MergeMetaCreatorPtr(new IndexTableMergeMetaCreator(schema, reclaimMapCreator, splitStrategyFactory));
    case tt_kv:
    case tt_kkv:
        return MergeMetaCreatorPtr(new KeyValueMergeMetaCreator(schema));
    default:
        assert(false);
    }
    return MergeMetaCreatorPtr();
}
}} // namespace indexlib::merger
