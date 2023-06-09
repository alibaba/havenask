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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_SEGMENT_BUILDER_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_SEGMENT_BUILDER_H

#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"

#include "indexlib_plugin/plugins/aitheta/segment_builder.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/raw_segment.h"
#include "indexlib_plugin/plugins/aitheta/index_segment.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_config.h"

namespace indexlib {
namespace aitheta_plugin {

class IndexSegmentBuilder : public SegmentBuilder {
 public:
    IndexSegmentBuilder(const SchemaParameter &schemaParameter, const KnnConfig &knnCfg)
        : SegmentBuilder(schemaParameter),
          mRawSegment(new RawSegment(SegmentMeta(SegmentType::kRaw, false, schemaParameter.dimension))),
          mKnnCfg(knnCfg) {}
    ~IndexSegmentBuilder() = default;

 public:
    bool Init() override;
    bool Build(catid_t catid, docid_t docid, const embedding_t embedding) override;
    bool Dump(const indexlib::file_system::DirectoryPtr &indexDir) override;

 public:
    bool BatchBuild(const std::vector<SegmentPtr> &sgts, const indexlib::file_system::DirectoryPtr &dir,
                    const std::set<catid_t> &buildingCatids);

 public:
    size_t EstimateCurrentMemoryUse() override { return mRawSegment->Size(); }
    size_t EstimateDumpFileSize() override { return mRawSegment->Size() * REDUCE_MEM_USE_MAGIC_NUM; }
    size_t EstimateTempMemoryUseInDump() override;

 private:
    bool BuildIndex(EmbedHolderPtr &embedHolder, IndexSegment &indexSgt);
    const std::string BuildTmpDumpDir();

 private:
    RawSegmentPtr mRawSegment;
    KnnConfig mKnnCfg;

 private:
    METRIC_DECLARE(mTrainIndexLatency);
    METRIC_DECLARE(mBuildIndexLatency);
    METRIC_DECLARE(mDumpIndexLatency);
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexSegmentBuilder);

}
}

#endif  //__INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_SEGMENT_BUILDER_H
