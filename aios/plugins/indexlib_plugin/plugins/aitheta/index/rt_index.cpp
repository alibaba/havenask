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
#include "indexlib_plugin/plugins/aitheta/index/rt_index.h"

using namespace std;
using namespace aitheta;
using namespace autil;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, RtIndex);

bool RtIndex::Search(const Query& q, const std::function<bool(uint64_t docid)>& docIdFilter,
                     aitheta::IndexSearcher::Context*& rawCtx) {
    ContextPtr res;
    if (!cxtHolder->Get(signature, rawCtx)) {
        res = streamer->createContext(indexParams);
        if (!res) {
            IE_LOG(ERROR, "failed to create ctx, return null");
            return false;
        }
        rawCtx = res.get();
        cxtHolder->Add(signature, rawCtx);
    } else {
        res.reset(rawCtx);
    }
    res->setFilter(docIdFilter);

    if (unlikely(enableGpu || q.queryNum > 1u)) {
        IE_LOG(ERROR, "streamer doesn't support gpu search or query num > 1");
        res.release();
        return false;
    }
    int32_t ret = 0;
    if (unlikely(q.isLrSearch)) {
        ret = streamer->linearSearch(q.topk, q.embedding.get(), q.dimension, res);
    } else {
        ret = streamer->knnSearch(q.topk, q.embedding.get(), q.dimension, res);
    }

    if (unlikely(ret != 0)) {
        const char* error = IndexError::What(ret);
        IE_LOG(WARN, "error[%s], failed to search[%s]", error, q.toString().c_str());
        res.release();
        return false;
    }
    res.release();

    IE_LOG(DEBUG, "rt_index has searched a query with catid[%ld]", q.catid);
    return true;
}

bool RtIndex::Build(docid_t docid, const EmbeddingPtr& embedding, uint32_t dimension) {
    ContextPtr res;
    IndexSearcher::Context* rawCtx = nullptr;
    if (!cxtHolder->Get(signature, rawCtx)) {
        res = streamer->createContext(indexParams);
        if (!res) {
            IE_LOG(ERROR, "failed to build as failure of context creation");
            return false;
        }
        cxtHolder->Add(signature, res.get());
    } else {
        res.reset(rawCtx);
    }

    EmbeddingPtr modifiedEmbed = embedding;
    int32_t modifiedDim = dimension;
    if (reformer.m() > 0u) {
        // TODO(richard.sy): abstract out to a function
        aitheta::Vector<float> vec;
        reformer.transFeature(embedding.get(), dimension, &vec);
        MALLOC(modifiedEmbed, vec.size(), float);
        std::copy(vec.data(), vec.data() + vec.size(), modifiedEmbed.get());
        modifiedDim += reformer.m();
    }

    int32_t ret = streamer->addVector(docid, modifiedEmbed.get(), modifiedDim, res);
    res.release();
    if (ret != 0) {
        const char* error = IndexError::What(ret);
        IE_LOG(WARN, "error[%s], failed to build docid[%d]", error, docid);
        return false;
    }

    // thread unsafe,but Search does not use it
    ++docNum;

    IE_LOG(DEBUG, "rt_index has built a doc with docid[%d]", docid);
    return true;
}

}
}