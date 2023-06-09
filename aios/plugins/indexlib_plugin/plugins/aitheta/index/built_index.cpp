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
#include "indexlib_plugin/plugins/aitheta/index/built_index.h"

using namespace std;
using namespace aitheta;
using namespace autil;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, BuiltIndex);

bool BuiltIndex::Build(docid_t id, const EmbeddingPtr& embedding, uint32_t dimension) {
    INDEXLIB_FATAL_ERROR(UnImplement, "BuiltIndex does not support the function");
    return false;
}

bool BuiltIndex::Search(const Query& q, const FilterFunc& docIdFilter, IndexSearcher::Context*& rawCtx) {
    ContextPtr res;

    if (!cxtHolder->Get(signature, rawCtx)) {
        res = searcher->createContext(indexParams);
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

    int32_t ret = 0;
    if (enableGpu) {
        if (unlikely(q.isLrSearch)) {
            ret = searcher->linearSearch(q.topk, q.embedding.get(), q.dimension, q.queryNum, res);
        } else {
            ret = searcher->knnSearch(q.topk, q.embedding.get(), q.dimension, q.queryNum, res);
        }
    } else {
        if (unlikely(q.queryNum != 1u)) {
            IE_LOG(ERROR, "disable gpu while batch query num is[%u]", q.queryNum);
            res.release();
            return false;
        }
        if (unlikely(q.isLrSearch)) {
            ret = searcher->linearSearch(q.topk, q.embedding.get(), q.dimension, res);
        } else {
            ret = searcher->knnSearch(q.topk, q.embedding.get(), q.dimension, res);
        }
    }
    if (unlikely(ret != 0)) {
        const char* error = IndexError::What(ret);
        IE_LOG(WARN, "error[%s], failed to search[%s]", error, q.toString().c_str());
        res.release();
        return false;
    }
    res.release();
    return true;
}

}
}