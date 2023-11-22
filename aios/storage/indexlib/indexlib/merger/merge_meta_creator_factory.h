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
#pragma once

#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/merger/merge_meta_creator.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

DECLARE_REFERENCE_CLASS(merger, SplitSegmentStrategyFactory);

namespace indexlib { namespace merger {

class MergeMetaCreatorFactory
{
public:
    MergeMetaCreatorFactory();
    ~MergeMetaCreatorFactory();

public:
    static MergeMetaCreatorPtr Create(config::IndexPartitionSchemaPtr& schema,
                                      const index::ReclaimMapCreatorPtr& reclaimMapCreator,
                                      SplitSegmentStrategyFactory* splitStrategyFactory);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeMetaCreatorFactory);
}} // namespace indexlib::merger
