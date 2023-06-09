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
#ifndef __INDEXLIB_DOCUMENT_DEDUPER_CREATOR_H
#define __INDEXLIB_DOCUMENT_DEDUPER_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/document_deduper/document_deduper.h"
#include "indexlib/partition/modifier/partition_modifier.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);

namespace indexlib { namespace partition {

class DocumentDeduperCreator
{
public:
    DocumentDeduperCreator();
    ~DocumentDeduperCreator();

public:
    static DocumentDeduperPtr CreateBuildingSegmentDeduper(const config::IndexPartitionSchemaPtr& schema,
                                                           const config::IndexPartitionOptions& options,
                                                           const index_base::InMemorySegmentPtr& inMemSegment,
                                                           const partition::PartitionModifierPtr& modifier);

    static DocumentDeduperPtr CreateBuiltSegmentsDeduper(const config::IndexPartitionSchemaPtr& schema,
                                                         const config::IndexPartitionOptions& options,
                                                         const partition::PartitionModifierPtr& modifier);

private:
    static bool CanCreateDeduper(const config::IndexPartitionSchemaPtr& schema,
                                 const config::IndexPartitionOptions& options,
                                 const partition::PartitionModifierPtr& modifier);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentDeduperCreator);
}} // namespace indexlib::partition

#endif //__INDEXLIB_DOCUMENT_DEDUPER_CREATOR_H
