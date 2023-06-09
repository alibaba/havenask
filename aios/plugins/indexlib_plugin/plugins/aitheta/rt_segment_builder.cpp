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
#include "indexlib_plugin/plugins/aitheta/rt_segment_builder.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include <aitheta/index_framework.h>

#include "indexlib_plugin/plugins/aitheta/util/custom_work_item.h"
#include "indexlib_plugin/plugins/aitheta/util/output_storage.h"
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_params_selector_factory.h"

using namespace aitheta;
using namespace std;
using namespace autil;

namespace indexlib {
namespace aitheta_plugin {
using namespace indexlib::file_system;
IE_LOG_SETUP(aitheta_plugin, RtSegmentBuilder);

RtSegmentBuilder::~RtSegmentBuilder() {
    if (mThreadPool) {
        mThreadPool->stop();
    }
}

bool RtSegmentBuilder::Init() {
    SegmentMeta meta(SegmentType::kRtIndex, false, mSchemaParam.dimension);
    mSegment.reset(new RtSegment(meta, mSchemaParam));

    uint32_t threadNum = 1u;
    uint32_t queueSize = mSchemaParam.rtOption.queueSize;
    mThreadPool.reset(new autil::ThreadPool(threadNum, queueSize));
    if (!mThreadPool->start("aitheta_rt")) {
        IE_LOG(INFO, "failed to start pool, threadNum[%u], queueSize[%u]", threadNum, queueSize);
        return false;
    }

    METRIC_SETUP(mBuildRtLatency, "BuildRtLatency", kmonitor::GAUGE);
    METRIC_SETUP(mDumpRtSgtLatency, "DumpRtSgtLatency", kmonitor::GAUGE);
    METRIC_SETUP(mBuildRtSuccessQps, "BuildRtSuccessQps", kmonitor::QPS);
    METRIC_SETUP(mBuildRtSkippedQps, "BuildRtSkippedQps", kmonitor::QPS);

    IE_LOG(INFO, "successfully initialize real-time segment builder");
    return true;
}

bool RtSegmentBuilder::Build(catid_t catid, docid_t docid, const EmbeddingPtr embedding) {
    if (likely(mSegment->IsAvailable())) {
        auto buildFunc = [&, catid, docid, embedding]() {
            {
                ScopedLatencyReporter reporter(mBuildRtLatency);
                mSegment->Add(catid, docid, embedding);
            }
            QPS_REPORT(mBuildRtSuccessQps);
        };

        CustomWorkItem* workItem = new CustomWorkItem(buildFunc);
        bool isBlock = mSchemaParam.rtOption.skipDoc ? false : true;
        if (!CustomWorkItem::Run(workItem, mThreadPool, isBlock)) {
            QPS_REPORT(mBuildRtSkippedQps);
        }
    } else {
        IE_LOG(ERROR, "rt sgt is not available, failed to build rt doc");
        return false;
    }
    return true;
}

bool RtSegmentBuilder::Dump(const indexlib::file_system::DirectoryPtr& dir) {
    ScopedLatencyReporter reporter(mDumpRtSgtLatency);
    mThreadPool->stop();
    return mSegment->Dump(dir);
}

}
}
