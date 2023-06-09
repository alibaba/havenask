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

#include <memory>

#include "indexlib/index/inverted_index/format/DictInlinePostingDecoder.h"
#include "indexlib/index/inverted_index/format/InMemDocListDecoder.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {

class InMemDictInlineDocListDecoder : public InMemDocListDecoder
{
public:
    InMemDictInlineDocListDecoder(autil::mem_pool::Pool* sessionPool, bool isReferenceCompress)
        : InMemDocListDecoder(sessionPool, isReferenceCompress)
        , _sessionPool(sessionPool)
        , _dictInlineDecoder(nullptr)
    {
    }

    ~InMemDictInlineDocListDecoder()
    {
        if (_dictInlineDecoder) {
            IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _dictInlineDecoder);
        }
    }

    void Init(const PostingFormatOption& postingFormatOption, int64_t dictInlinePostingData);
    bool DecodeDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                         ttf_t& currentTTF) override;
    bool DecodeCurrentTFBuffer(tf_t* tfBuffer) override;
    void DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer) override;
    void DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer) override;

private:
    autil::mem_pool::Pool* _sessionPool;
    DictInlinePostingDecoder* _dictInlineDecoder;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
