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
#include "indexlib_plugin/plugins/aitheta/util/embed_holder_iterator/raw_sgt_holder_iterator_impl.h"

using namespace std;
namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, RawSgtHolderIteratorImpl);

EmbedHolderPtr RawSgtHolderIteratorImpl::Value() {
    auto holder = mCatId2EmbedHolder.begin()->second;

    if (isValueReady) {
        return holder;
    }

    auto& embedInfos = holder->GetEmbedVec();
    if (mDocIdMap) {
        for (auto& embedInfo : embedInfos) {
            docid_t& docid = embedInfo.docid;
            docid = mDocIdMap->GetNewDocId(docid);
        }
    } else {
        IE_LOG(INFO, "DocIdMap does not exist");
    }

    size_t num = 0u;
    for (size_t i = 0u; i < embedInfos.size(); ++i) {
        if (INVALID_DOCID != embedInfos[i].docid) {
            embedInfos[num] = embedInfos[i];
            ++num;
        }
    }
    embedInfos.resize(num);
    isValueReady = true;
    return holder;
}

void RawSgtHolderIteratorImpl::Next() {
    if (!mCatId2EmbedHolder.empty()) {
        mCatId2EmbedHolder.erase(mCatId2EmbedHolder.begin());
    }
    isValueReady = false;
}

bool RawSgtHolderIteratorImpl::IsValid() const { return !mCatId2EmbedHolder.empty(); }

}
}
