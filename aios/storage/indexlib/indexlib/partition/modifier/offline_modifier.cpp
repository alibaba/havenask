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
#include "indexlib/partition/modifier/offline_modifier.h"

#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OfflineModifier);

void OfflineModifier::Init(const PartitionDataPtr& partitionData, const BuildResourceMetricsPtr& buildResourceMetrics)
{
    PatchModifier::Init(partitionData, buildResourceMetrics);
    mDeletionMapReader = partitionData->GetDeletionMapReader();
    assert(mDeletionMapReader);
}

bool OfflineModifier::RemoveDocument(docid_t docId)
{
    if (!PatchModifier::RemoveDocument(docId)) {
        return false;
    }
    mDeletionMapReader->Delete(docId);
    return true;
}
}} // namespace indexlib::partition
