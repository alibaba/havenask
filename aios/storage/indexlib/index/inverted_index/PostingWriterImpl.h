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

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/format/DictInlineFormatter.h"
#include "indexlib/index/inverted_index/format/DocListEncoder.h"
#include "indexlib/index/inverted_index/format/InMemPostingDecoder.h"
#include "indexlib/index/inverted_index/format/PositionListEncoder.h"
#include "indexlib/index/inverted_index/format/PostingFormat.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::index {
class TermMatchData;
struct SingleDocInfo {
    tf_t tf;
    fieldmap_t fieldMap;
};

union DocListUnionStruct {
    uint64_t dictInlineValue;
    SingleDocInfo singleDocInfo;
};

enum DocListType { DLT_SINGLE_DOC_INFO = 0, DLT_DOC_LIST_ENCODER = 1, DLT_DICT_INLINE_VALUE = 2 };

enum class SpeedUpType { NONE, BITMAP };

class PostingWriterImpl : public PostingWriter
{
public:
    /* keep kisableDictInline for khronos_table index writer */
    PostingWriterImpl(index::PostingWriterResource* writerResource, bool disableDictInline = false)
        : _docListEncoder(NULL)
        , _positionListEncoder(NULL)
        , _writerResource(writerResource)
        , _termPayload(0)
        , _disableDictInline(disableDictInline)
    {
        assert(_writerResource->byteSlicePool);
        if (_writerResource->postingFormatOption.HasPositionList()) {
            _positionListEncoder = IE_POOL_NEW_CLASS(
                _writerResource->byteSlicePool, PositionListEncoder,
                _writerResource->postingFormatOption.GetPosListFormatOption(), _writerResource->byteSlicePool,
                _writerResource->bufferPool,
                _writerResource->postingFormat ? _writerResource->postingFormat->GetPositionListFormat() : NULL,
                _writerResource->postingFormatOption.GetFormatVersion());
        }
        if (disableDictInline || _writerResource->postingFormatOption.HasPositionList() ||
            _writerResource->postingFormatOption.HasTfBitmap()) {
            _docListEncoder = IE_POOL_NEW_CLASS(
                _writerResource->byteSlicePool, DocListEncoder,
                _writerResource->postingFormatOption.GetDocListFormatOption(), _writerResource->simplePool,
                _writerResource->byteSlicePool, _writerResource->bufferPool,
                _writerResource->postingFormat ? _writerResource->postingFormat->GetDocListFormat() : NULL,
                _writerResource->postingFormatOption.GetDocListCompressMode(),
                _writerResource->postingFormatOption.GetFormatVersion());
            _docListType = DLT_DOC_LIST_ENCODER;
        } else {
            _docListUnion.singleDocInfo.fieldMap = 0;
            _docListUnion.singleDocInfo.tf = 0;
            _docListType = DLT_SINGLE_DOC_INFO;
        }

        auto format = _writerResource->postingFormatOption;
        _costPerDoc = sizeof(docid32_t) + (format.HasTermFrequency() ? sizeof(tf_t) : 0) +
                      (format.HasDocPayload() ? sizeof(docpayload_t) : 0) +
                      (format.HasFieldMap() ? sizeof(fieldmap_t) : 0);
        _costPerPosition = sizeof(pos_t) + (format.HasPositionPayload() ? sizeof(pospayload_t) : 0);
        _compressRatio =
            _writerResource->postingFormatOption.GetDocListCompressMode() == PFOR_DELTA_COMPRESS_MODE ? 0.8 : 1;
    }

    PostingWriterImpl(const PostingWriterImpl& postingWriterImpl) = delete;
    ~PostingWriterImpl()
    {
        if (_positionListEncoder) {
            _positionListEncoder->~PositionListEncoder();
            _positionListEncoder = NULL;
        }

        if (_docListEncoder) {
            _docListEncoder->~DocListEncoder();
            _docListEncoder = NULL;
        }
    }

public:
    void AddPosition(pos_t pos, pospayload_t posPayload, int32_t fieldIdxInPack) override;

    void EndDocument(docid32_t docId, docpayload_t docPayload) override
    {
        if (_docListType == DLT_SINGLE_DOC_INFO) {
            // encode singleDocInfo
            DictInlineFormatter formatter(_writerResource->postingFormatOption);
            formatter.SetTermPayload(_termPayload);
            formatter.SetDocId(docId);
            formatter.SetDocPayload(docPayload);
            formatter.SetTermFreq(_docListUnion.singleDocInfo.tf);
            formatter.SetFieldMap(_docListUnion.singleDocInfo.fieldMap);
            uint64_t inlinePostingValue;
            if (formatter.GetDictInlineValue(inlinePostingValue)) {
                _docListUnion.dictInlineValue = inlinePostingValue;
                MEMORY_BARRIER();
                _docListType = DLT_DICT_INLINE_VALUE;
            } else {
                UseDocListEncoder(_docListUnion.singleDocInfo.tf, _docListUnion.singleDocInfo.fieldMap, docId,
                                  docPayload);
            }
        } else if (_docListType == DLT_DICT_INLINE_VALUE) {
            assert(_docListEncoder == NULL);
            DictInlineFormatter formatter(_writerResource->postingFormatOption, _docListUnion.dictInlineValue);

            UseDocListEncoder(formatter.GetTermFreq(), formatter.GetFieldMap(), formatter.GetDocId(),
                              formatter.GetDocPayload());
            _docListEncoder->EndDocument(docId, docPayload);
        } else {
            assert(_docListType == DLT_DOC_LIST_ENCODER);
            _docListEncoder->EndDocument(docId, docPayload);
        }

        if (_positionListEncoder) {
            _positionListEncoder->EndDocument();
        }
        _notExistInCurrentDoc = true;
        _estimateDumpTempMemSize += _costPerDoc;
    }

    void EndDocument(docid32_t docId, docpayload_t docPayload, fieldmap_t fieldMap) override
    {
        SetFieldMap(fieldMap);
        EndDocument(docId, docPayload);
    }

    void EndSegment() override;

    void Dump(const file_system::FileWriterPtr& file) override;
    uint32_t GetDumpLength() const override;

    // termpayload need for temp store
    termpayload_t GetTermPayload() const override { return _termPayload; }

    void SetTermPayload(termpayload_t payload) override { _termPayload = payload; }

    bool CreateReorderPostingWriter(autil::mem_pool::Pool* pool, const std::vector<docid32_t>* newOrder,
                                    PostingWriter* output) const override;

    uint32_t GetDF() const override
    {
        switch (_docListType) {
        case DLT_SINGLE_DOC_INFO:
            return 0;
        case DLT_DOC_LIST_ENCODER:
            return _docListEncoder->GetDF();
        case DLT_DICT_INLINE_VALUE:
            return 1;
        default:
            assert(false);
            return 0;
        }
    }

    uint32_t GetTotalTF() const override
    {
        switch (_docListType) {
        case DLT_SINGLE_DOC_INFO:
            return _docListUnion.singleDocInfo.tf;
        case DLT_DOC_LIST_ENCODER:
            return _docListEncoder->GetTotalTF();
        case DLT_DICT_INLINE_VALUE:
            return DictInlineFormatter(_writerResource->postingFormatOption, _docListUnion.dictInlineValue)
                .GetTermFreq();
        default:
            assert(false);
            return 0;
        }
    }

    tf_t GetCurrentTF() const
    {
        switch (_docListType) {
        case DLT_SINGLE_DOC_INFO:
            return _docListUnion.singleDocInfo.tf;
        case DLT_DOC_LIST_ENCODER:
            return _docListEncoder->GetCurrentTF();
        case DLT_DICT_INLINE_VALUE:
            return 0;
        default:
            assert(false);
            return 0;
        }
    }

    /* for only tf without position list */
    void SetCurrentTF(tf_t tf) override
    {
        _notExistInCurrentDoc = tf <= 0;
        switch (_docListType) {
        case DLT_SINGLE_DOC_INFO:
            _docListUnion.singleDocInfo.tf = tf;
            break;
        case DLT_DOC_LIST_ENCODER:
            _docListEncoder->SetCurrentTF(tf);
            break;
        case DLT_DICT_INLINE_VALUE: {
            DictInlineFormatter formatter(_writerResource->postingFormatOption, _docListUnion.dictInlineValue);
            UseDocListEncoder(formatter.GetTermFreq(), formatter.GetFieldMap(), formatter.GetDocId(),
                              formatter.GetDocPayload());
            _docListEncoder->SetCurrentTF(tf);
            break;
        }
        default:
            assert(false);
        }
    }

    // get from encoder
    bool NotExistInCurrentDoc() const override
    {
        assert(_notExistInCurrentDoc == (GetCurrentTF() == 0));
        return _notExistInCurrentDoc;
    }

    const util::ByteSliceList* GetPositionList() const override;

    PositionBitmapWriter* GetPositionBitmapWriter() const override
    {
        if (_docListEncoder) {
            return _docListEncoder->GetPositionBitmapWriter();
        }
        return NULL;
    }

    void SetFieldMap(fieldmap_t fieldMap)
    {
        switch (_docListType) {
        case DLT_SINGLE_DOC_INFO:
            _docListUnion.singleDocInfo.fieldMap = fieldMap;
            break;
        case DLT_DOC_LIST_ENCODER:
            _docListEncoder->SetFieldMap(fieldMap);
            break;
        case DLT_DICT_INLINE_VALUE: {
            DictInlineFormatter formatter(_writerResource->postingFormatOption, _docListUnion.dictInlineValue);
            UseDocListEncoder(formatter.GetTermFreq(), formatter.GetFieldMap(), formatter.GetDocId(),
                              formatter.GetDocPayload());
            _docListEncoder->SetFieldMap(fieldMap);
            break;
        }
        default:
            assert(false);
        }
    }

    InMemPostingDecoder* CreateInMemPostingDecoder(autil::mem_pool::Pool* sessionPool) const override;

    docid32_t GetLastDocId() const override
    {
        switch (_docListType) {
        case DLT_SINGLE_DOC_INFO:
            return 0;
        case DLT_DOC_LIST_ENCODER:
            return _docListEncoder->GetLastDocId();
        case DLT_DICT_INLINE_VALUE:
            return DictInlineFormatter(_writerResource->postingFormatOption, _docListUnion.dictInlineValue).GetDocId();
        default:
            assert(false);
            return INVALID_DOCID;
        }
    }

    docpayload_t GetLastDocPayload() const override
    {
        switch (_docListType) {
        case DLT_SINGLE_DOC_INFO:
            return 0;
        case DLT_DOC_LIST_ENCODER:
            return _docListEncoder->GetLastDocPayload();
        case DLT_DICT_INLINE_VALUE:
            return DictInlineFormatter(_writerResource->postingFormatOption, _docListUnion.dictInlineValue)
                .GetDocPayload();
        default:
            assert(false);
            return INVALID_DOC_PAYLOAD;
        }
    }

    bool GetDictInlinePostingValue(uint64_t& inlinePostingValue, bool& isDocList) const override;

public:
    // for test
    fieldmap_t GetFieldMap() const override
    {
        switch (_docListType) {
        case DLT_SINGLE_DOC_INFO:
            return _docListUnion.singleDocInfo.fieldMap;
        case DLT_DOC_LIST_ENCODER:
            return _docListEncoder->GetFieldMap();
        case DLT_DICT_INLINE_VALUE:
            return 0;
        default:
            assert(false);
            return 0;
        }
    }
    DocListEncoder* GetDocListEncoder() const { return _docListEncoder; }

    size_t GetEstimateDumpTempMemSize() const override;

    index::PostingWriterResource* TEST_GetWriterResource() const { return _writerResource; }

private:
    void UseDocListEncoder(tf_t tf, fieldmap_t fieldmap, docid32_t docId, docpayload_t docPayload);

    void BuildReorderPostingWriterBySortDocIdOnly(autil::mem_pool::Pool* pool, PostingIterator* iter,
                                                  const std::vector<docid32_t>* newOrder,
                                                  PostingWriterImpl* writer) const;

    void BuildReorderPostingWriterBySort(autil::mem_pool::Pool* pool, PostingIterator* iter,
                                         const std::vector<docid32_t>* newOrder, PostingWriterImpl* writer) const;

    void BuildReorderPostingWriterByBitmap(autil::mem_pool::Pool* pool, PostingIterator* iter,
                                           const std::vector<docid32_t>* newOrder, PostingWriterImpl* writer) const;

    void ReorderPostingWriterAddDoc(docid32_t docId, TermMatchData* termMatchData, PostingWriterImpl* writer,
                                    bool isLastDoc) const;
    bool NeedBitmapSpeedUp(uint32_t bitmapCapacity) const;
    bool NeedTermMatchData() const;

private:
    DocListUnionStruct volatile _docListUnion;
    DocListEncoder* volatile _docListEncoder;
    // pointer: PositionListEncoder may not exist
    PositionListEncoder* _positionListEncoder;
    PostingWriterResource* _writerResource;
    termpayload_t _termPayload;
    DocListType volatile _docListType;
    bool _disableDictInline;
    bool _notExistInCurrentDoc {true};

    // for (sort) dump cost estimate
    size_t _costPerPosition = 0;
    size_t _costPerDoc = 0;
    size_t _estimateDumpTempMemSize = 0;
    float _compressRatio = 1;

private:
    friend class PostingWriterImplTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
