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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_RAW_SEGMENT_BUILDER_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_RAW_SEGMENT_BUILDER_H

#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"

#include "indexlib_plugin/plugins/aitheta/segment_builder.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/raw_segment.h"

namespace indexlib {
namespace aitheta_plugin {

class RawSegmentBuilder : public SegmentBuilder {
 public:
    RawSegmentBuilder(const SchemaParameter &schemaParameter)
        : SegmentBuilder(schemaParameter),
          mRawSegment(new RawSegment(SegmentMeta(SegmentType::kRaw, false, schemaParameter.dimension))) {}

    ~RawSegmentBuilder() {}

 public:
    bool Build(catid_t catid, docid_t docid, const EmbeddingPtr embedding) override;
    bool Dump(const indexlib::file_system::DirectoryPtr &indexDir) override;

 public:
    size_t EstimateCurrentMemoryUse() override { return mRawSegment->Size(); }
    size_t EstimateDumpFileSize() override { return mRawSegment->Size(); }
    size_t EstimateTempMemoryUseInDump() override { return 0ul; }

 private:
    RawSegmentPtr mRawSegment;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RawSegmentBuilder);

}
}

#endif  //__INDEXLIB_PLUGIN_PLUGINS_AITHETA_RAW_SEGMENT_BUILDER_H
