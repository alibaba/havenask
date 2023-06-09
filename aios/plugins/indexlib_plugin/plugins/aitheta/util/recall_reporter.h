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
#ifndef INDEXLIB_PLUGIN_PLUGINS_AITHETA_REC_REPORTER_H
#define INDEXLIB_PLUGIN_PLUGINS_AITHETA_REC_REPORTER_H

#include "autil/ThreadPool.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/util/metric_reporter.h"
#include "indexlib_plugin/plugins/aitheta/index_segment_reader.h"
#include "indexlib_plugin/plugins/aitheta/util/metric_reporter.h"

namespace indexlib {
namespace aitheta_plugin {

class RecallReporter {
 public:
    RecallReporter(std::vector<IndexSegmentReaderPtr>& readers, MetricReporterPtr& metricReporter, int32_t rtSgtNo,
                   uint32_t sampleInterval)
        : mSgtReaders(readers),
          _metricReporter(metricReporter),
          mSampleInterval(sampleInterval),
          mRtSgtNo(rtSgtNo),
          mTimer(0u) {}
    ~RecallReporter();

 public:
    bool Init();
    bool CanReport();
    bool Report(const std::vector<Query>& knnSearchQues);

 private:
    bool Calc(SearchResult& knnSearchResult, SearchResult& benchmarkResult, uint32_t topk, float& recall);

 private:
    std::vector<IndexSegmentReaderPtr>& mSgtReaders;
    MetricReporterPtr _metricReporter;
    const uint32_t mSampleInterval;
    const int32_t mRtSgtNo;
    uint32_t mTimer;
    std::shared_ptr<autil::ThreadPool> mThreadPool;
    autil::mem_pool::Pool mPool;
    IE_LOG_DECLARE();

 private:
    METRIC_DECLARE(mRtSgtRecall);
    METRIC_DECLARE(mFullSgtRecall);
    METRIC_DECLARE(mOverallRecall);
};

DEFINE_SHARED_PTR(RecallReporter);

inline bool RecallReporter::CanReport() {
    // not thread safe, but it is ok
    ++mTimer;
    return !(mTimer % mSampleInterval);
}

DEFINE_SHARED_PTR(RecallReporter);

}
}

#endif  // INDEXLIB_PLUGIN_PLUGINS_AITHETA_METRIC_REPORTER_H
