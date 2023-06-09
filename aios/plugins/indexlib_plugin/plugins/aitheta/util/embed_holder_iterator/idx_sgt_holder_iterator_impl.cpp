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
#include "indexlib_plugin/plugins/aitheta/util/embed_holder_iterator/idx_sgt_holder_iterator_impl.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_params_selector_factory.h"
#include "indexlib_plugin/plugins/aitheta/util/input_storage.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_searcher_factory.h"

using namespace std;
using namespace aitheta;
using namespace indexlib::file_system;
namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, IdxSgtHolderIteratorImpl);

EmbedHolderPtr IdxSgtHolderIteratorImpl::Value() {
    if (!mValue && !BuildValue()) {
        return EmbedHolderPtr();
    }
    return mValue;
}

void IdxSgtHolderIteratorImpl::Next() {
    ++mNext;
    mValue.reset();
}

bool IdxSgtHolderIteratorImpl::IsValid() const { return mNext < mIndexAttrHolder.Size(); }

bool IdxSgtHolderIteratorImpl::BuildValue() {
    mValue.reset();

    const auto &indexAttr = mIndexAttrHolder.Get(mNext);
    IndexParams indexParams;
    int64_t signature = 0u;
    bool enableGpu = false;
    // TODO(richard.sy): fix the bug when gpu is enabled
    IndexSearcherPtr searcher = KnnSearcherFactory::Create(mSchemaParam, mOfflineKnnCfg, indexAttr, false,
                                                           indexParams, signature, enableGpu);
    if (!searcher) {
        IE_LOG(ERROR, "failed to create searcher, index type[%s]", mSchemaParam.indexType.c_str());
        return false;
    }

    std::vector<int8_t> buffer(indexAttr.size);
    mIndexProvider->Read((void *)buffer.data(), indexAttr.size, indexAttr.offset).GetOrThrow();
    const int8_t *indexAddr = const_cast<int8_t *>(buffer.data());

    IndexStoragePtr storage(new InputStorage(indexAddr));
    int32_t ret = searcher->loadIndex(".", storage);
    if (ret < 0) {
        IE_LOG(ERROR, "failed to load index, error[%s]", IndexError::What(ret));
        return false;
    }

    int64_t catid = indexAttr.catid;
    int32_t dim = mSchemaParam.dimension;

    auto &reformer = indexAttr.reformer;
    bool enableMips = reformer.m() > 0u;
    if (enableMips) {
        dim += reformer.m();
    }

    auto holder = EmbedHolderPtr(new EmbedHolder(dim, catid, enableMips));
    auto iter = searcher->createIterator();
    if (!iter) {
        IE_LOG(ERROR, "failed to create iterator");
        return false;
    }

    for (; iter->isValid(); iter->next()) {
        docid_t docid = (docid_t)iter->key();

        docid = mDocIdMap->GetNewDocId(docid);
        if (INVALID_DOCID == docid) {
            continue;
        }
        EmbeddingPtr embedding;
        MALLOC(embedding, dim, float);
        memcpy((void *)embedding.get(), iter->data(), sizeof(float) * dim);
        holder->Add(docid, embedding);
    }

    if (enableMips && !holder->UndoMips(reformer)) {
        return false;
    }

    mValue = holder;
    return true;
}

}
}
