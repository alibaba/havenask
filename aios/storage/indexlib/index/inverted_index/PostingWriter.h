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

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/index/inverted_index/format/PostingFormat.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::index {

class PositionBitmapWriter;
class InMemPostingDecoder;

struct PostingWriterResource {
public:
    PostingWriterResource(util::SimplePool* _simplePool, autil::mem_pool::Pool* _byteSlicePool,
                          autil::mem_pool::RecyclePool* _bufferPool, PostingFormatOption _postingFormatOption)
        : simplePool(_simplePool)
        , byteSlicePool(_byteSlicePool)
        , bufferPool(_bufferPool)
        , postingFormatOption(_postingFormatOption)
        , postingFormat(new PostingFormat(_postingFormatOption))
    {
    }

    util::SimplePool* simplePool;
    autil::mem_pool::Pool* byteSlicePool;
    autil::mem_pool::RecyclePool* bufferPool;
    PostingFormatOption postingFormatOption;
    std::shared_ptr<PostingFormat> postingFormat;
};

class PostingWriter
{
public:
    PostingWriter() {}
    virtual ~PostingWriter() {}

public:
    virtual void AddPosition(pos_t pos, pospayload_t posPayload, fieldid_t fieldId) = 0;

    virtual void EndDocument(docid_t docId, docpayload_t docPayload) = 0;
    virtual void EndDocument(docid_t docId, docpayload_t docPayload, fieldmap_t fieldMap) = 0;

    virtual void EndSegment() = 0;

    virtual void Dump(const std::shared_ptr<file_system::FileWriter>& file) = 0;
    virtual bool CreateReorderPostingWriter(autil::mem_pool::Pool* pool, const std::vector<docid_t>* newOrder,
                                            PostingWriter* output) const = 0;

    virtual uint32_t GetDumpLength() const = 0;

    virtual uint32_t GetTotalTF() const = 0;
    virtual uint32_t GetDF() const = 0;
    virtual docpayload_t GetLastDocPayload() const { return INVALID_DOC_PAYLOAD; }
    virtual fieldmap_t GetFieldMap() const { return 0; }
    virtual const util::ByteSliceList* GetPositionList() const
    {
        assert(false);
        return NULL;
    }
    virtual PositionBitmapWriter* GetPositionBitmapWriter() const
    {
        assert(false);
        return NULL;
    }

    virtual bool NotExistInCurrentDoc() const = 0;
    virtual void SetTermPayload(termpayload_t payload) = 0;
    virtual termpayload_t GetTermPayload() const = 0;

    virtual InMemPostingDecoder* CreateInMemPostingDecoder(autil::mem_pool::Pool* sessionPool) const { return NULL; }

    virtual docid_t GetLastDocId() const { return INVALID_DOCID; }

    virtual bool GetDictInlinePostingValue(uint64_t& inlinePostingValue, bool& isDocList) const { return false; }

    virtual void SetCurrentTF(tf_t tf) = 0;

    virtual size_t GetEstimateDumpTempMemSize() const = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
