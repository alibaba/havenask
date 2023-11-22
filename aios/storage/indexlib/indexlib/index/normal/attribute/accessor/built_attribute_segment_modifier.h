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

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_modifier.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"
#include "indexlib/index/util/dump_item_typed.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class BuiltAttributeSegmentModifier : public AttributeSegmentModifier
{
public:
    BuiltAttributeSegmentModifier(segmentid_t segId, const config::AttributeSchemaPtr& attrSchema,
                                  util::BuildResourceMetrics* buildResourceMetrics);

    virtual ~BuiltAttributeSegmentModifier();

public:
    void Update(docid_t docId, attrid_t attrId, const autil::StringView& attrValue, bool isNull) override;

    void Dump(const file_system::DirectoryPtr& dir, segmentid_t srcSegment) override;

    void CreateDumpItems(const file_system::DirectoryPtr& directory, segmentid_t srcSegmentId,
                         std::vector<std::unique_ptr<common::DumpItem>>* dumpItems);

private:
    AttributeUpdaterPtr& GetUpdater(uint32_t idx);
    // virtual for test
    virtual AttributeUpdaterPtr CreateUpdater(uint32_t idx);

private:
    config::AttributeSchemaPtr mAttrSchema;
    segmentid_t mSegmentId;
    std::vector<AttributeUpdaterPtr> mUpdaters;
    util::BuildResourceMetrics* mBuildResourceMetrics;

private:
    friend class BuiltAttributeSegmentModifierTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuiltAttributeSegmentModifier);
}} // namespace indexlib::index
