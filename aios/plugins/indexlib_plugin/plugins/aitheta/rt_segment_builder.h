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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_RT_SEGMENT_BUILDER_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_RT_SEGMENT_BUILDER_H

#include <tuple>
#include <unordered_map>
#include "autil/ThreadPool.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include <aitheta/index_framework.h>

#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/rt_segment.h"
#include "indexlib_plugin/plugins/aitheta/segment_builder.h"
#include "indexlib_plugin/plugins/aitheta/segment_builder.h"
#include "indexlib_plugin/plugins/aitheta/util/metric_reporter.h"

namespace indexlib {
namespace aitheta_plugin {

class RtSegmentBuilder : public SegmentBuilder {
 public:
    RtSegmentBuilder(const SchemaParameter &schemaParameter) : SegmentBuilder(schemaParameter) {}
    ~RtSegmentBuilder();

 public:
    bool Init() override;
    bool Build(catid_t catid, docid_t docid, const EmbeddingPtr embedding) override;
    bool Dump(const indexlib::file_system::DirectoryPtr &dir) override;

 public:
    // TODO: take docs-in-mThreadPool into consideration
    size_t EstimateCurrentMemoryUse() override { return mSegment->IsAvailable() ? mSegment->Size() : 0u; }
    size_t EstimateDumpFileSize() override { return mSegment->IsAvailable() ? mSegment->Size() : 0u; }
    size_t EstimateTempMemoryUseInDump() override { return 0u; }

 public:
    RtSegmentPtr GetSegment() const { return mSegment; }

 private:
    RtSegmentPtr mSegment;
    std::shared_ptr<autil::ThreadPool> mThreadPool;
    IE_LOG_DECLARE();

 private:
    METRIC_DECLARE(mBuildRtLatency);
    METRIC_DECLARE(mDumpRtSgtLatency);
    METRIC_DECLARE(mBuildRtSuccessQps);
    METRIC_DECLARE(mBuildRtSkippedQps);
};

DEFINE_SHARED_PTR(RtSegmentBuilder);

}
}

#endif  //__INDEXLIB_PLUGIN_PLUGINS_AITHETA_IN_MEMORY_SEGMENT_BUILDER_H
