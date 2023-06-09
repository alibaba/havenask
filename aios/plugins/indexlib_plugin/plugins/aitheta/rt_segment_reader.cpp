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
#include "indexlib_plugin/plugins/aitheta/rt_segment_reader.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_streamer_factory.h"

using namespace std;
using namespace aitheta;
using namespace indexlib::index;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, RtSegmentReader);

bool RtSegmentReader::Init(const ContextHolderPtr &holder, docid_t sgtBase, const DeletionMapReaderPtr &deletionMapRr) {
    mSgtBase = sgtBase;
    if (deletionMapRr && (mSchemaParam.rtOption.enable || mSchemaParam.incOption.enable)) {
        mDocidFilter = [=](uint64_t docid) { return deletionMapRr->IsDeleted((docid_t)(docid + mSgtBase)); };
    }

    if (!mCatid2OfflineMaxIndexAttr.empty()) {
        for (const auto &kv : mCatid2OfflineMaxIndexAttr) {
            if (!InitRtIndex(kv.first, kv.second, holder)) {
                return false;
            }
        }
    } else {
        // Only support index without category field
        float norm = mSchemaParam.rtOption.mipsNorm;
        size_t num = mSchemaParam.rtOption.docNumPredict;
        if (norm <= 0.0f) {
            IE_LOG(ERROR, "mips norm is invalid when pure-rt enabled");
            return false;
        }
        catid_t catid = DEFAULT_CATEGORY_ID;
        IndexAttr attr(num, MipsReformer(norm));
        if (!InitRtIndex(catid, attr, holder)) {
            return false;
        }
        IE_LOG(INFO, "offline segments are empty, pure-rt is enabled");
    }

    auto rtSegment = DYNAMIC_POINTER_CAST(RtSegment, mSegment);
    if (!rtSegment) {
        IE_LOG(ERROR, "failed to cast Segment to RtSegment");
        return false;
    }
    rtSegment->SetOnlineIndexHolder(mOnlineIndexHolder);

    IE_LOG(INFO, "finish initializing rt index segment reader");
    return true;
}

bool RtSegmentReader::InitRtIndex(catid_t catid, const IndexAttr &indexAttr, const ContextHolderPtr &holder) {
    IndexParams indexParams;
    int64_t signature = 0l;

    auto streamer = KnnStreamerFactory::Create(mSchemaParam, KnnConfig(), indexAttr, indexParams, signature);
    if (!streamer) {
        return false;
    }

    IndexAttr rtindexAttr(0u, indexAttr.reformer);
    OnlineIndexPtr rtIndex(new RtIndex(streamer, rtindexAttr, signature, indexParams, holder));
    if (!mOnlineIndexHolder->Add(catid, rtIndex)) {
        IE_LOG(ERROR, "failed to add index, catid[%ld] ", catid);
        return false;
    }

    IE_LOG(DEBUG, "initialized RtIndex with catid[%ld]", catid);
    return true;
}

}
}
