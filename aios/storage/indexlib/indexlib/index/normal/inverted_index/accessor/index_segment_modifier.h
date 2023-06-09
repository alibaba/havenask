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

#include <memory>
#include <vector>

#include "autil/ConstString.h"
#include "indexlib/common/dump_item.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(index, PatchIndexSegmentUpdaterBase);
DECLARE_REFERENCE_CLASS(document, ModifiedTokens);

namespace indexlib { namespace index {

class IndexSegmentModifier
{
public:
    IndexSegmentModifier(segmentid_t segId, const config::IndexSchemaPtr& indexSchema,
                         util::BuildResourceMetrics* buildResourceMetrics);

public:
    void Update(docid_t docId, indexid_t indexId, const document::ModifiedTokens& modifiedTokens);

    void Dump(const file_system::DirectoryPtr& dir, segmentid_t srcSegment);

    void CreateDumpItems(const file_system::DirectoryPtr& directory, segmentid_t srcSegmentId,
                         std::vector<std::unique_ptr<common::DumpItem>>* dumpItems);

private:
    const PatchIndexSegmentUpdaterBasePtr& GetUpdater(indexid_t indexId);
    PatchIndexSegmentUpdaterBasePtr CreateUpdater(indexid_t indexId);

private:
    config::IndexSchemaPtr _indexSchema;
    segmentid_t _segmentId;
    std::vector<PatchIndexSegmentUpdaterBasePtr> _updaters;
    util::BuildResourceMetrics* _buildResourceMetrics;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexSegmentModifier);
}} // namespace indexlib::index
