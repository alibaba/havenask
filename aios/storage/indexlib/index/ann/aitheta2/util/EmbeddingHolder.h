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
#pragma once

#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingBuffer.h"

namespace indexlibv2::index::ann {

class EmbeddingHolder
{
public:
    EmbeddingHolder(const aitheta2::IndexMeta& meta, bool hasMultiIndexId = true)
        : _meta(meta)
        , _hasMultiIndexId(hasMultiIndexId)
    {
    }
    ~EmbeddingHolder() {}

public:
    bool Add(const char* embedding, docid_t docId, index_id_t indexId);
    bool Add(const char* embedding, docid_t docId, const std::vector<index_id_t>& indexIds);
    bool Steal(EmbeddingHolder& rhs);
    bool Delete(docid_t docId, const std::vector<index_id_t>& indexIds);
    void Clear();

    const std::map<index_id_t, std::shared_ptr<EmbeddingBufferBase>>& GetBufferMap() const;
    std::map<index_id_t, std::shared_ptr<EmbeddingBufferBase>>& GetMutableBufferMap();
    void SetDocIdMapper(const std::vector<docid_t>* old2NewDocId);
    const aitheta2::IndexMeta& GetMeta() const;
    bool HasMultiIndexId() const;

private:
    aitheta2::IndexMeta _meta;
    bool _hasMultiIndexId = true;
    std::map<index_id_t, std::shared_ptr<EmbeddingBufferBase>> _bufferMap;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
