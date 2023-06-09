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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_SEGMENT_READER_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_SEGMENT_READER_H

#include <unordered_map>
#include <pthread.h>
#include "autil/ThreadPool.h"
#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/index_segment.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_config.h"
#include "indexlib_plugin/plugins/aitheta/util/search_util.h"
#include "indexlib_plugin/plugins/aitheta/index/built_index.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_params_selector_factory.h"

namespace indexlib {
namespace aitheta_plugin {

typedef aitheta::IndexSearcher::Document Document;

class IndexSegmentReader {
 public:
    IndexSegmentReader(const util::KeyValueMap &kvParam, const SegmentPtr &segment)
        : mSchemaParam(kvParam),
          mSgtBase(INVALID_DOCID),
          mSegment(segment),
          mOnlineIndexHolder(new OnlineIndexHolder) {}
    virtual ~IndexSegmentReader() = default;

 public:
    virtual bool Init(const ContextHolderPtr &holder, docid_t sgtBase,
                      const indexlib::index::DeletionMapReaderPtr &deletionMapRr);
    virtual bool Search(const std::vector<Query> &queries, SearchResult &result);

    const OfflineIndexAttrHolder &GetOfflineIndexAttrHolder() const;
    SegmentPtr GetSegment() const { return mSegment; }
    void SetMetricReporter(const MetricReporterPtr &metricReporter) { _metricReporter = metricReporter; }
    void InitQueryTopkRatio(const std::unordered_map<int64_t, float> &topkRatios) { mCatid2TopkRatio = topkRatios; }

 protected:
    void ModifyTopk(catid_t catid, uint32_t srcTopk, uint32_t &dstTok) const;
    bool ModifyQuery(const std::vector<Query> &srcs, std::vector<Query> &dsts);
    bool SequentialSearch(const std::vector<Query> &queries, SearchResult &result);
    bool ConcurrentSearch(const std::vector<Query> &queries, SearchResult &result);
    bool Search(const Query &query, SearchResult &result);

 protected:
    bool InitBuiltIndex(const OfflineIndexAttr &indexAttr, const KnnConfig &knnCfg, const ContextHolderPtr &holder,
                        const int8_t *indexAddrBase);

 protected:
    SchemaParameter mSchemaParam;
    docid_t mSgtBase;
    FilterFunc mDocidFilter;
    SegmentPtr mSegment;
    OnlineIndexHolderPtr mOnlineIndexHolder;
    MetricReporterPtr _metricReporter;
    std::unordered_map<int64_t, float> mCatid2TopkRatio;

    IE_LOG_DECLARE();
};

inline void IndexSegmentReader::ModifyTopk(catid_t catid, uint32_t srcTopk, uint32_t &dstTopk) const {
    auto iterator = mCatid2TopkRatio.find(catid);
    if (iterator == mCatid2TopkRatio.end()) {
        dstTopk = 0u;
    } else {
        dstTopk = std::max((uint32_t)(iterator->second * srcTopk), 1u);
    }
    IE_LOG(DEBUG, "src topk[%u], dst topk[%u]", srcTopk, dstTopk);
}

DEFINE_SHARED_PTR(IndexSegmentReader);

}
}

#endif  //__INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_SEGMENT_READER_H
