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
#include "indexlib_plugin/plugins/aitheta/util/recall_reporter.h"
#include "indexlib_plugin/plugins/aitheta/util/custom_work_item.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, RecallReporter);

RecallReporter::~RecallReporter() {
    if (mThreadPool) {
        mThreadPool->stop();
    }
}

bool RecallReporter::Init() {
    if (!mSampleInterval) {
        IE_LOG(ERROR, "mSampleInterval should NOT be zero");
        return false;
    }

    uint32_t threadNum = 1u;
    uint32_t queueSize = 1u;
    mThreadPool.reset(new autil::ThreadPool(threadNum, queueSize));
    if (!mThreadPool->start("aitheta_rec")) {
        IE_LOG(INFO, "failed to start pool, threadNum[%u], queueSize[%u]", threadNum, queueSize);
        return false;
    }
    METRIC_SETUP(mRtSgtRecall, "RtSgtRecall", kmonitor::GAUGE);
    METRIC_SETUP(mFullSgtRecall, "FullSgtRecall", kmonitor::GAUGE);
    METRIC_SETUP(mOverallRecall, "OverallRecall", kmonitor::GAUGE);
    return true;
}

bool RecallReporter::Report(const std::vector<Query>& knnSearchQs) {
    auto reportFunc = [&, knnSearchQs]() {
        std::vector<Query> benchmarkQs(knnSearchQs);
        for (auto& q : benchmarkQs) {
            q.isLrSearch = true;
        }
        uint32_t recallNum = benchmarkQs.size() * benchmarkQs[0].topk;

        SearchResult knnSearchRes;
        SearchResult benchmarkRes;
        for (int32_t sgtNo = 0; sgtNo < (int32_t)mSgtReaders.size(); ++sgtNo) {
            SearchResult knnSearchSgtRes;
            mSgtReaders[sgtNo]->Search(knnSearchQs, knnSearchSgtRes);
            for (auto& doc : *(knnSearchSgtRes.GetDocs())) {
                knnSearchRes.AddUnsafe(doc.first, doc.second);
            }

            SearchResult benchmarkSgtRes;
            mSgtReaders[sgtNo]->Search(benchmarkQs, benchmarkSgtRes);
            for (auto& doc : *(benchmarkSgtRes.GetDocs())) {
                benchmarkRes.AddUnsafe(doc.first, doc.second);
            }

            float sgtRecall = 0.0f;
            if (Calc(knnSearchSgtRes, benchmarkSgtRes, recallNum, sgtRecall)) {
                if (mRtSgtNo == sgtNo) {
                    METRIC_REPORT(mRtSgtRecall, sgtRecall);
                } else if (0 == sgtNo) {
                    METRIC_REPORT(mFullSgtRecall, sgtRecall);
                }
            }
        }
        float recall = 0.0f;
        if (Calc(knnSearchRes, benchmarkRes, recallNum, recall)) {
            METRIC_REPORT(mOverallRecall, recall);
        }
    };

    return CustomWorkItem::Run(new CustomWorkItem(reportFunc), mThreadPool);
}

bool RecallReporter::Calc(SearchResult& knnSearchRes, SearchResult& benchmarkRes, uint32_t topk, float& recall) {
    MatchInfoPtr benchmarkMatchInfo(new MatchInfo);
    benchmarkRes.SelectTopk(topk, benchmarkMatchInfo, &mPool);
    uint32_t benchmarkDocNum = benchmarkMatchInfo->matchCount;
    if (benchmarkDocNum <= 0) {
        return false;
    }

    MatchInfoPtr knnSearchMatchInfo(new MatchInfo);
    knnSearchRes.SelectTopk(topk, knnSearchMatchInfo, &mPool);
    uint32_t knnSearchDocNum = knnSearchMatchInfo->matchCount;

    uint32_t hitNum = 0u;

    for (size_t bidx = 0u, kidx = 0u; bidx < benchmarkDocNum && kidx < knnSearchDocNum;) {
        if (knnSearchMatchInfo->docIds[kidx] == benchmarkMatchInfo->docIds[bidx]) {
            ++kidx;
            ++bidx;
            ++hitNum;
        } else if (knnSearchMatchInfo->docIds[kidx] < benchmarkMatchInfo->docIds[bidx]) {
            ++kidx;
        } else {
            ++bidx;
        }
    }

    recall = hitNum / (float)benchmarkDocNum;

    return true;
}

}
}
