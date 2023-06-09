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
#include "indexlib/index/common/numeric_compress/ReferenceCompressIntEncoder.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"

namespace indexlib::index {

class OneDocMerger
{
private:
    static constexpr int64_t DOCID_EOF = std::numeric_limits<int64_t>::max();

private:
    enum LastReadFrom {
        INVALID,
        PATCH,
        BUFFERED_POSTING,
        BOTH,
    };

private:
    class NextResult
    {
    public:
        NextResult(docid_t docId, bool isEof) : _docId(docId), _isEof(isEof) {}
        NextResult() : _docId(0), _isEof(true) {}

        docid_t DocId() const { return _docId; }
        bool IsEof() const { return _isEof; }

        bool operator<(const NextResult& other) const
        {
            if (_isEof) {
                return false;
            }
            if (other._isEof) {
                return true;
            }
            return _docId < other._docId;
        }
        bool operator>(const NextResult& other) const { return other < *this; }

    private:
        docid_t _docId;
        bool _isEof;
    };

public:
    OneDocMerger(const PostingFormatOption& formatOption, PostingDecoderImpl* decoder,
                 SingleTermIndexSegmentPatchIterator* patchIter);
    ~OneDocMerger();

public:
    // docid_t NextDoc();
    void Merge(docid_t newDocId, PostingWriterImpl* postingWriter);

    docid_t CurrentDoc() const;
    bool Next();

private:
    tf_t MergePosition(docid_t newDocId, PostingWriterImpl* postingWriter);

    NextResult PatchCurrent() const;
    bool PatchEnd() const;
    NextResult PatchNext();
    NextResult BufferedPostingCurrent() const;
    bool BufferedPostingEnd() const;
    NextResult BufferedPostingNext();
    bool BookkeepReadFrom(const NextResult& postingResult, const NextResult& patchResult, LastReadFrom* lastReadFrom);

private:
    docid_t _docIdBuf[MAX_DOC_PER_RECORD];
    tf_t _tfListBuf[MAX_DOC_PER_RECORD];
    docpayload_t _docPayloadBuf[MAX_DOC_PER_RECORD];
    fieldmap_t _fieldMapBuffer[MAX_DOC_PER_RECORD];
    pos_t _posBuf[MAX_POS_PER_RECORD];
    pospayload_t _posPayloadBuf[MAX_POS_PER_RECORD];

    uint32_t _docCountInBuf;
    uint32_t _docBufCursor;
    uint32_t _lastDocBufCursor;
    docid_t _docId;

    uint32_t _posCountInBuf;
    uint32_t _posBufCursor;
    uint32_t _posIndex;

    PostingDecoderImpl* _decoder;
    SingleTermIndexSegmentPatchIterator* _patchIterator;
    ComplexDocId _patchDoc;
    LastReadFrom _lastReadFrom;
    PostingFormatOption _formatOption;

    ReferenceCompressIntReader<uint32_t> _bufReader;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
