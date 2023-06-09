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
#ifndef __INDEXLIB_OFFLINE_MODIFIER_H
#define __INDEXLIB_OFFLINE_MODIFIER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/modifier/patch_modifier.h"

namespace indexlib { namespace partition {

class OfflineModifier : public PatchModifier
{
public:
    OfflineModifier(const config::IndexPartitionSchemaPtr& schema, bool enablePackFile)
        : PatchModifier(schema, enablePackFile, true)
    {
    }
    ~OfflineModifier() {}

public:
    void Init(const index_base::PartitionDataPtr& partitionData,
              const util::BuildResourceMetricsPtr& buildResourceMetrics = util::BuildResourceMetricsPtr()) override;

    bool RemoveDocument(docid_t docId) override;

private:
    index::DeletionMapReaderPtr mDeletionMapReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineModifier);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OFFLINE_MODIFIER_H
