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
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"

#include "indexlib/index/inverted_index/SectionAttributeReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BufferedPostingIterator);

BufferedPostingIterator::BufferedPostingIterator(const PostingFormatOption& postingFormatOption,
                                                 autil::mem_pool::Pool* sessionPool,
                                                 indexlib::util::PooledUniquePtr<InvertedIndexSearchTracer> tracer)
    : PostingIteratorImpl(sessionPool, std::move(tracer))
    , _postingFormatOption(postingFormatOption)
    , _lastDocIdInBuffer(INVALID_DOCID - 1)
    , _currentDocId(INVALID_DOCID)
    , _docBufferCursor(NULL)
    , _currentTTF(0)
    , _tfBufferCursor(0)
    , _tfBuffer(NULL)
    , _docPayloadBuffer(NULL)
    , _fieldMapBuffer(NULL)
    , _decoder(NULL)
    , _needMoveToCurrentDoc(false)
    , _inDocPosIterInited(false)
    , _inDocPositionIterator(NULL)
    , _state(0, postingFormatOption.GetPosListFormatOption())
{
    AllocateBuffers();
    _docBufferBase = _docBuffer;
}

BufferedPostingIterator::~BufferedPostingIterator()
{
    IE_POOL_COMPATIBLE_DELETE_VECTOR(_sessionPool, _tfBuffer, MAX_DOC_PER_RECORD);
    IE_POOL_COMPATIBLE_DELETE_VECTOR(_sessionPool, _docPayloadBuffer, MAX_DOC_PER_RECORD);
    IE_POOL_COMPATIBLE_DELETE_VECTOR(_sessionPool, _fieldMapBuffer, MAX_DOC_PER_RECORD);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _decoder);
}

bool BufferedPostingIterator::Init(const std::shared_ptr<SegmentPostingVector>& segPostings,
                                   const SectionAttributeReader* sectionReader, const uint32_t statePoolSize)
{
    if (!PostingIteratorImpl::Init(segPostings)) {
        return false;
    }
    _decoder = CreateBufferedPostingDecoder();
    _decoder->Init(segPostings);
    _statePool.Init(statePoolSize);
    _state.SetSectionReader(sectionReader);
    return true;
}

BufferedPostingDecoder* BufferedPostingIterator::CreateBufferedPostingDecoder()
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, BufferedPostingDecoder, &_state, _sessionPool);
}

docid_t BufferedPostingIterator::SeekDoc(docid_t docId)
{
    docid_t ret = INVALID_DOCID;
    auto ec = InnerSeekDoc(docId, ret);
    index::ThrowIfError(ec);
    return ret;
}

index::ErrorCode BufferedPostingIterator::SeekDocWithErrorCode(docid_t docId, docid_t& result)
{
    return InnerSeekDoc(docId, result);
}

void BufferedPostingIterator::Unpack(TermMatchData& termMatchData)
{
    DecodeTFBuffer();
    DecodeDocPayloadBuffer();
    DecodeFieldMapBuffer();
    if (_postingFormatOption.HasDocPayload()) {
        termMatchData.SetHasDocPayload(true);
        termMatchData.SetDocPayload(InnerGetDocPayload());
    }
    if (_postingFormatOption.HasFieldMap()) {
        termMatchData.SetFieldMap(InnerGetFieldMap());
    }
    if (_needMoveToCurrentDoc) {
        MoveToCurrentDoc();
    }
    if (_postingFormatOption.HasTermFrequency()) {
        NormalInDocState* state = _statePool.Alloc();
        _state.Clone(state);
        termMatchData.SetInDocPositionState(state);
    } else {
        termMatchData.SetMatched(true);
    }
}

index::ErrorCode BufferedPostingIterator::SeekPositionWithErrorCode(pos_t pos, pos_t& result)
{
    if (_needMoveToCurrentDoc) {
        try {
            MoveToCurrentDoc();
        } catch (const util::FileIOException& e) {
            return index::ErrorCode::FileIO;
        }
    }

    if (!_inDocPosIterInited) {
        if (_postingFormatOption.HasPositionList()) {
            _inDocPosIterInited = true;
            _inDocPositionIterator = _decoder->GetPositionIterator();
            try {
                _inDocPositionIterator->Init(_state);
            } catch (const util::FileIOException& e) {
                return index::ErrorCode::FileIO;
            }
        } else {
            result = INVALID_POSITION;
            return index::ErrorCode::OK;
        }
    }
    return _inDocPositionIterator->SeekPositionWithErrorCode(pos, result);
}

pospayload_t BufferedPostingIterator::GetPosPayload()
{
    assert(_inDocPositionIterator);
    return _inDocPositionIterator->GetPosPayload();
}

BufferedPostingIterator* BufferedPostingIterator::Clone() const
{
    auto tracer = indexlib::util::MakePooledUniquePtr<PostingIteratorImpl::TracerPtr::ValueType>(_sessionPool);
    BufferedPostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, BufferedPostingIterator,
                                                                 _postingFormatOption, _sessionPool, std::move(tracer));
    assert(iter != NULL);
    // in block cache mode: BlockByteSliceList already have first few block handles (by Lookup),
    //  this following Init have no file io inside, shuold not throw FileIOException;
    if (!iter->Init(mSegmentPostings, _state.GetSectionReader(), _statePool.GetElemCount())) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, iter);
        iter = NULL;
    }
    return iter;
}

void BufferedPostingIterator::Reset()
{
    if (!mSegmentPostings || mSegmentPostings->size() == 0) {
        return;
    }

    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _decoder);
    _decoder = CreateBufferedPostingDecoder();
    _decoder->Init(mSegmentPostings);

    _currentDocId = INVALID_DOCID;
    _lastDocIdInBuffer = INVALID_DOCID - 1;
    _needMoveToCurrentDoc = false;
    _inDocPosIterInited = false;
}
} // namespace indexlib::index
