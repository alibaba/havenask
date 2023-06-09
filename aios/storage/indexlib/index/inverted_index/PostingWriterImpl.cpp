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
#include "indexlib/index/inverted_index/PostingWriterImpl.h"

#include <limits>
#include <memory>

#include "autil/memory.h"
#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/format/DictInlineEncoder.h"
#include "indexlib/index/inverted_index/format/DictInlineFormatter.h"
#include "indexlib/index/inverted_index/format/InMemDictInlineDocListDecoder.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PostingWriterImpl);

using DocIdWithTMDIndex = std::pair<docid_t, int32_t>;

void PostingWriterImpl::AddPosition(pos_t pos, pospayload_t posPayload, int32_t fieldIdxInPack)
{
    if (_docListType == DLT_SINGLE_DOC_INFO) {
        if (_writerResource->postingFormatOption.HasFieldMap()) {
            assert(fieldIdxInPack < 8);
            assert(fieldIdxInPack >= 0);
            _docListUnion.singleDocInfo.fieldMap = _docListUnion.singleDocInfo.fieldMap | (1 << fieldIdxInPack);
        }
        _docListUnion.singleDocInfo.tf = _docListUnion.singleDocInfo.tf + 1;
    } else if (_docListType == DLT_DICT_INLINE_VALUE) {
        DictInlineFormatter formatter(_writerResource->postingFormatOption, _docListUnion.dictInlineValue);

        UseDocListEncoder(formatter.GetTermFreq(), formatter.GetFieldMap(), formatter.GetDocId(),
                          formatter.GetDocPayload());
        _docListEncoder->AddPosition(fieldIdxInPack);
    } else {
        assert(_docListType == DLT_DOC_LIST_ENCODER);
        _docListEncoder->AddPosition(fieldIdxInPack);
    }
    _notExistInCurrentDoc = false;

    if (_positionListEncoder) {
        _positionListEncoder->AddPosition(pos, posPayload);
        _estimateDumpTempMemSize += _costPerPosition;
    }
}

void PostingWriterImpl::EndSegment()
{
    if (_docListType == DLT_DICT_INLINE_VALUE) {
        return;
    }
    if (_docListEncoder) {
        assert(_docListType == DLT_DOC_LIST_ENCODER);
        _docListEncoder->Flush();
    }
    if (_positionListEncoder) {
        _positionListEncoder->Flush();
    }
}

uint32_t PostingWriterImpl::GetDumpLength() const
{
    if (!_docListEncoder) {
        return 0;
    }

    uint64_t inlineValue;
    bool isDocList = false;
    if (GetDictInlinePostingValue(inlineValue, isDocList)) {
        assert(isDocList);
        return 0;
    }

    uint32_t length = _docListEncoder->GetDumpLength();
    if (_positionListEncoder) {
        length += _positionListEncoder->GetDumpLength();
    }
    return length;
}

size_t PostingWriterImpl::GetEstimateDumpTempMemSize() const
{
    size_t _sortDumpMemSize =
        GetDF() * (NeedTermMatchData() ? (sizeof(DocIdWithTMDIndex) + sizeof(TermMatchData)) : sizeof(docid_t));
    return _estimateDumpTempMemSize * _compressRatio + _sortDumpMemSize;
}

void PostingWriterImpl::Dump(const file_system::FileWriterPtr& file)
{
    if (!_docListEncoder) {
        return;
    }

    uint64_t inlineValue;
    bool isDocList = false;
    if (GetDictInlinePostingValue(inlineValue, isDocList)) {
        assert(isDocList);
        return;
    }

    _docListEncoder->Dump(file);
    if (_positionListEncoder) {
        _positionListEncoder->Dump(file);
    }
}

const util::ByteSliceList* PostingWriterImpl::GetPositionList() const
{
    if (_positionListEncoder) {
        return _positionListEncoder->GetPositionList();
    }
    return NULL;
}

InMemPostingDecoder* PostingWriterImpl::CreateInMemPostingDecoder(autil::mem_pool::Pool* sessionPool) const
{
    InMemPostingDecoder* postingDecoder = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, InMemPostingDecoder);
    InMemDocListDecoder* docListDecoder = NULL;
    if (_docListType != DLT_DOC_LIST_ENCODER) {
        InMemDictInlineDocListDecoder* inMemDictInlineDecoder =
            IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, InMemDictInlineDocListDecoder, sessionPool,
                                         _writerResource->postingFormatOption.IsReferenceCompress());
        if (_docListType == DLT_DICT_INLINE_VALUE) {
            inMemDictInlineDecoder->Init(_writerResource->postingFormatOption, _docListUnion.dictInlineValue);
        }
        docListDecoder = inMemDictInlineDecoder;
    } else {
        assert(_docListType == DLT_DOC_LIST_ENCODER);
        // Doc List -> Position List
        docListDecoder = _docListEncoder->GetInMemDocListDecoder(sessionPool);
    }
    postingDecoder->SetDocListDecoder(docListDecoder);

    if (_positionListEncoder != NULL) {
        InMemPositionListDecoder* positionListDecoder = _positionListEncoder->GetInMemPositionListDecoder(sessionPool);
        postingDecoder->SetPositionListDecoder(positionListDecoder);
    }
    return postingDecoder;
}

bool PostingWriterImpl::GetDictInlinePostingValue(uint64_t& inlinePostingValue, bool& isDocList) const
{
    InlinePostingValueWithOrder inlinePostingValueWithOrder;
    if (_disableDictInline) {
        return false;
    }

    if (_docListType == DLT_DOC_LIST_ENCODER) {
        if (_positionListEncoder != nullptr || _writerResource->postingFormatOption.GetFormatVersion() < 1 ||
            !_docListEncoder->IsDocIdContinuous() || !_writerResource->postingFormatOption.IsOnlyDocId()) {
            return false;
        }
        docid_t lastDocid = _docListEncoder->GetLastDocId();
        df_t df = _docListEncoder->GetDF();
        if (!DictInlineEncoder::EncodeContinuousDocId(lastDocid - df + 1, df, /*enableDictInlineLongDF*/ false,
                                                      inlinePostingValueWithOrder)) {
            return false;
        }
        inlinePostingValue = inlinePostingValueWithOrder.second;
        isDocList = true;
        return true;
    }

    if (_docListType == DLT_DICT_INLINE_VALUE) {
        inlinePostingValue = _docListUnion.dictInlineValue;
        isDocList = false;
        return true;
    }

    assert(_docListType == DLT_SINGLE_DOC_INFO);
    INDEXLIB_FATAL_ERROR(InconsistentState, "cannot return dictInlineValue when df = 0");
    return false;
}

void PostingWriterImpl::UseDocListEncoder(tf_t tf, fieldmap_t fieldMap, docid_t docId, docpayload_t docPayload)
{
    assert(_docListEncoder == NULL);
    DocListEncoder* docListEncoder = IE_POOL_NEW_CLASS(
        _writerResource->byteSlicePool, DocListEncoder, _writerResource->postingFormatOption.GetDocListFormatOption(),
        _writerResource->simplePool, _writerResource->byteSlicePool, _writerResource->bufferPool,
        _writerResource->postingFormat ? _writerResource->postingFormat->GetDocListFormat() : NULL,
        _writerResource->postingFormatOption.GetDocListCompressMode(),
        _writerResource->postingFormatOption.GetFormatVersion());
    assert(docListEncoder);
    docListEncoder->SetCurrentTF(tf);
    docListEncoder->SetFieldMap(fieldMap);
    docListEncoder->EndDocument(docId, docPayload);
    _docListEncoder = docListEncoder;
    MEMORY_BARRIER();
    _docListType = DLT_DOC_LIST_ENCODER;
}

void PostingWriterImpl::ReorderPostingWriterAddDoc(docid_t docId, TermMatchData* termMatchData,
                                                   PostingWriterImpl* writer, bool isLastDoc) const
{
    assert(termMatchData);
    auto posInDocIter = termMatchData->GetInDocPositionIterator();
    if (posInDocIter) {
        // we have position list, we can add position to rebuild posting list
        for (pos_t pos = posInDocIter->SeekPosition(0); pos != INVALID_POSITION;
             pos = posInDocIter->SeekPosition(pos + 1)) {
            writer->AddPosition(pos, posInDocIter->GetPosPayload(), 0 /* for fake */);
        }
    } else {
        // we don't have position list
        // if tf list exists, we will get current tf correctly (total tf is correct too
        // otherwise we should correct total tf
        size_t currentTF = isLastDoc ? GetTotalTF() - writer->GetTotalTF() : termMatchData->GetTermFreq();
        writer->SetCurrentTF(currentTF);
    }
    docpayload_t docPayload = termMatchData->HasDocPayload() ? termMatchData->GetDocPayload() : 0;
    fieldmap_t fieldMap = termMatchData->HasFieldMap() ? termMatchData->GetFieldMap() : 0;
    writer->EndDocument(docId, docPayload, fieldMap);
}

void PostingWriterImpl::BuildReorderPostingWriterByBitmap(autil::mem_pool::Pool* pool, PostingIterator* postingIter,
                                                          const std::vector<docid_t>* newOrder,
                                                          PostingWriterImpl* writer) const
{
    assert(!NeedTermMatchData());
    util::ExpandableBitmap newDocs(newOrder->size(), false, pool);
    writer->SetTermPayload(GetTermPayload());

    docid_t maxDocId = 0;
    for (docid_t docId = postingIter->SeekDoc(INVALID_DOCID); docId != INVALID_DOCID;
         docId = postingIter->SeekDoc(docId)) {
        docid_t newDocId = newOrder->at(docId);
        newDocs.Set(newDocId);
        maxDocId = std::max(maxDocId, newDocId);
    }

    for (auto docId = newDocs.Begin(); docId != util::Bitmap::INVALID_INDEX; docId = newDocs.Next(docId)) {
        if (unlikely(docId == maxDocId)) {
            writer->SetCurrentTF(GetTotalTF() - writer->GetTotalTF());
        }
        writer->EndDocument(docId, 0 /*for fake*/, 0 /*for fake*/);
    }
}

void PostingWriterImpl::BuildReorderPostingWriterBySort(autil::mem_pool::Pool* pool, PostingIterator* postingIter,
                                                        const std::vector<docid_t>* newOrder,
                                                        PostingWriterImpl* writer) const
{
    if (!NeedTermMatchData()) {
        return BuildReorderPostingWriterBySortDocIdOnly(pool, postingIter, newOrder, writer);
    }
    autil::mem_pool::pool_allocator<DocIdWithTMDIndex> allocator(pool);
    autil::mem_pool::pool_allocator<TermMatchData> tmdAllocator(pool);

    std::vector<DocIdWithTMDIndex, decltype(allocator)> docInfos(allocator);
    std::vector<TermMatchData, decltype(tmdAllocator)> termMatchDatas(tmdAllocator);
    docInfos.reserve(GetDF());
    termMatchDatas.resize(GetDF());

    for (docid_t docId = postingIter->SeekDoc(INVALID_DOCID); docId != INVALID_DOCID;
         docId = postingIter->SeekDoc(docId)) {
        size_t tmdIndex = docInfos.size();
        postingIter->Unpack(termMatchDatas[tmdIndex]);
        docInfos.push_back({newOrder->at(docId), tmdIndex});
    }

    std::sort(docInfos.begin(), docInfos.end(), [](auto& lhs, auto& rhs) { return /*docid*/ lhs.first < rhs.first; });
    writer->SetTermPayload(GetTermPayload());
    for (size_t i = 0; i < docInfos.size(); ++i) {
        const auto& [docId, tmdId] = docInfos[i];
        bool isLastDoc = i + 1 == docInfos.size();
        ReorderPostingWriterAddDoc(docId, &termMatchDatas[tmdId], writer, isLastDoc);
    }
}

void PostingWriterImpl::BuildReorderPostingWriterBySortDocIdOnly(autil::mem_pool::Pool* pool,
                                                                 PostingIterator* postingIter,
                                                                 const std::vector<docid_t>* newOrder,
                                                                 PostingWriterImpl* writer) const
{
    autil::mem_pool::pool_allocator<docid_t> allocator(pool);
    std::vector<docid_t, decltype(allocator)> docIds(allocator);
    docIds.reserve(GetDF());
    for (docid_t docId = postingIter->SeekDoc(INVALID_DOCID); docId != INVALID_DOCID;
         docId = postingIter->SeekDoc(docId)) {
        docIds.push_back(newOrder->at(docId));
    }
    sort(docIds.begin(), docIds.end());
    writer->SetTermPayload(GetTermPayload());
    for (size_t i = 0; i < docIds.size(); ++i) {
        bool isLastDoc = i + 1 == docIds.size();
        if (unlikely(isLastDoc)) {
            writer->SetCurrentTF(GetTotalTF() - writer->GetTotalTF());
        }
        writer->EndDocument(docIds[i], 0 /*for fake*/, 0 /*for fake*/);
    }
}

bool PostingWriterImpl::NeedBitmapSpeedUp(uint32_t bitmapCapacity) const
{
    uint32_t df = GetDF();
    return df * log(df) * 8 >= bitmapCapacity && !NeedTermMatchData();
}

bool PostingWriterImpl::NeedTermMatchData() const
{
    return !_writerResource->postingFormatOption.IsOnlyDocId() &&
           !_writerResource->postingFormatOption.IsOnlyTermPayLoad();
}

bool PostingWriterImpl::CreateReorderPostingWriter(autil::mem_pool::Pool* pool, const std::vector<docid_t>* newOrder,
                                                   PostingWriter* output) const
{
    // TODO (yiping.typ) : support tf bitmap
    assert(!_writerResource->postingFormatOption.HasTfBitmap());
    PostingWriterImpl* writer = dynamic_cast<PostingWriterImpl*>(output);
    assert(writer);

    // in this case, we only use class @autil::mem_pool::Pool,
    // it's deallocate function do nothing to match @autil::unique_ptr_pool
    // we hope this pool is a temp pool (session pool)
    assert(pool && typeid(pool) == typeid(autil::mem_pool::Pool*));
    auto postingIter = autil::unique_ptr_pool(pool, POOL_COMPATIBLE_NEW_CLASS(pool, BufferedPostingIterator,
                                                                              _writerResource->postingFormatOption,
                                                                              pool, /*tracer=*/nullptr));
    SegmentPosting inMemSegPosting(_writerResource->postingFormatOption);
    inMemSegPosting.Init(0 /*baseDocId*/, 0, (PostingWriter*)this);
    std::shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector {inMemSegPosting});
    if (!postingIter->Init(segPostings, nullptr, GetDF())) {
        return false;
    }

    if (NeedBitmapSpeedUp(newOrder->size())) {
        BuildReorderPostingWriterByBitmap(pool, postingIter.get(), newOrder, writer);
    } else {
        BuildReorderPostingWriterBySort(pool, postingIter.get(), newOrder, writer);
    }

    writer->EndSegment();
    return true;
}

} // namespace indexlib::index
