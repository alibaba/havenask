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
#include "indexlib/index/inverted_index/BufferedIndexDecoder.h"

#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/format/DictInlinePostingDecoder.h"
#include "indexlib/index/inverted_index/format/InMemPostingDecoder.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/ShortListSegmentDecoder.h"
#include "indexlib/index/inverted_index/format/SkipListSegmentDecoder.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"
#include "indexlib/index/inverted_index/format/skiplist/PairValueSkipListReader.h"
#include "indexlib/index/inverted_index/format/skiplist/TriValueSkipListReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BufferedIndexDecoder);

BufferedIndexDecoder::~BufferedIndexDecoder()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _segmentDecoder);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _inDocPositionIterator);
}

void BufferedIndexDecoder::Init(const std::shared_ptr<SegmentPostingVector>& segPostings)
{
    assert(segPostings);
    _segPostings = segPostings;
    _segmentCount = (uint32_t)_segPostings->size();
    MoveToSegment(INVALID_DOCID);
}

bool BufferedIndexDecoder::DecodeDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId,
                                           docid_t& lastDocId, ttf_t& currentTTF)
{
    while (true) {
        if (DecodeDocBufferInOneSegment(startDocId, docBuffer, firstDocId, lastDocId, currentTTF)) {
            return true;
        }
        if (!MoveToSegment(startDocId)) {
            return false;
        }
    }
    return false;
}

bool BufferedIndexDecoder::DecodeDocBufferMayCopy(docid_t startDocId, docid_t*& docBuffer, docid_t& firstDocId,
                                                  docid_t& lastDocId, ttf_t& currentTTF)
{
    while (true) {
        if (DecodeDocBufferInOneSegmentMayCopy(startDocId, docBuffer, firstDocId, lastDocId, currentTTF)) {
            return true;
        }
        if (!MoveToSegment(startDocId)) {
            return false;
        }
    }
    return false;
}

bool BufferedIndexDecoder::DecodeCurrentTFBuffer(tf_t* tfBuffer)
{
    if (_needDecodeTF) {
        _segmentDecoder->DecodeCurrentTFBuffer(tfBuffer);
        _needDecodeTF = false;
        return true;
    }
    return false;
}

void BufferedIndexDecoder::DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer)
{
    if (_needDecodeDocPayload) {
        _segmentDecoder->DecodeCurrentDocPayloadBuffer(docPayloadBuffer);
        _needDecodeDocPayload = false;
    }
}

void BufferedIndexDecoder::DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer)
{
    if (!_needDecodeFieldMap) {
        return;
    }
    _segmentDecoder->DecodeCurrentFieldMapBuffer(fieldBitmapBuffer);
    _needDecodeFieldMap = false;
}

bool BufferedIndexDecoder::DecodeDocBufferInOneSegment(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId,
                                                       docid_t& lastDocId, ttf_t& currentTTF)
{
    docid_t nextSegBaseDocId = GetSegmentBaseDocId(_segmentCursor);
    if (nextSegBaseDocId != INVALID_DOCID && startDocId >= nextSegBaseDocId) {
        // start docid not in current segment
        return false;
    }

    docid_t curSegDocId = std::max(docid_t(0), startDocId - _baseDocId);
    if (!_segmentDecoder->DecodeDocBuffer(curSegDocId, docBuffer, firstDocId, lastDocId, currentTTF)) {
        return false;
    }
    _needDecodeTF = mCurSegPostingFormatOption.HasTfList();
    _needDecodeDocPayload = mCurSegPostingFormatOption.HasDocPayload();
    _needDecodeFieldMap = mCurSegPostingFormatOption.HasFieldMap();

    firstDocId += _baseDocId;
    lastDocId += _baseDocId;
    return true;
}

bool BufferedIndexDecoder::DecodeDocBufferInOneSegmentMayCopy(docid_t startDocId, docid_t*& docBuffer,
                                                              docid_t& firstDocId, docid_t& lastDocId,
                                                              ttf_t& currentTTF)
{
    docid_t nextSegBaseDocId = GetSegmentBaseDocId(_segmentCursor);
    if (nextSegBaseDocId != INVALID_DOCID && startDocId >= nextSegBaseDocId) {
        // start docid not in current segment
        return false;
    }

    docid_t curSegDocId = std::max(docid_t(0), startDocId - _baseDocId);
    if (!_segmentDecoder->DecodeDocBufferMayCopy(curSegDocId, docBuffer, firstDocId, lastDocId, currentTTF)) {
        return false;
    }
    _needDecodeTF = mCurSegPostingFormatOption.HasTfList();
    _needDecodeDocPayload = mCurSegPostingFormatOption.HasDocPayload();
    _needDecodeFieldMap = mCurSegPostingFormatOption.HasFieldMap();

    firstDocId += _baseDocId;
    lastDocId += _baseDocId;
    return true;
}

bool BufferedIndexDecoder::MoveToSegment(docid_t startDocId)
{
    uint32_t locateSegCursor = LocateSegment(_segmentCursor, startDocId);
    if (locateSegCursor >= _segmentCount) {
        return false;
    }

    _segmentCursor = locateSegCursor;
    SegmentPosting& curSegPosting = (*_segPostings)[_segmentCursor];
    mCurSegPostingFormatOption = curSegPosting.GetPostingFormatOption();
    _baseDocId = curSegPosting.GetBaseDocId();

    uint8_t compressMode = curSegPosting.GetCompressMode();
    uint8_t docCompressMode = ShortListOptimizeUtil::GetDocCompressMode(compressMode);
    if (docCompressMode == DICT_INLINE_COMPRESS_MODE) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _segmentDecoder);
        _segmentDecoder = IE_POOL_COMPATIBLE_NEW_CLASS(
            _sessionPool, DictInlinePostingDecoder, mCurSegPostingFormatOption,
            curSegPosting.GetDictInlinePostingData(), curSegPosting.IsDocListDictInline(), curSegPosting.IsDfFirst());

        ++_segmentCursor;
        return true;
    }

    const PostingWriter* postingWriter = curSegPosting.GetInMemPostingWriter();
    if (postingWriter) {
        InMemPostingDecoder* postingDecoder = postingWriter->CreateInMemPostingDecoder(_sessionPool);
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _segmentDecoder);
        _segmentDecoder = postingDecoder->GetInMemDocListDecoder();

        if (mCurSegPostingFormatOption.HasPositionList()) {
            InMemPositionListDecoder* posDecoder = postingDecoder->GetInMemPositionListDecoder();
            assert(posDecoder);
            _inDocStateKeeper.MoveToSegment(posDecoder);
            IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _inDocPositionIterator);
            _inDocPositionIterator = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, NormalInDocPositionIterator,
                                                                  mCurSegPostingFormatOption.GetPosListFormatOption());
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, postingDecoder);
        ++_segmentCursor;
        return true;
    }

    file_system::ByteSliceReader docListReader;
    util::ByteSlice* singleSlice = curSegPosting.GetSingleSlice();
    util::ByteSliceList* postingList = curSegPosting.GetSliceListPtr().get();
    // do not use doclist reader to read, because reader can't seek back.
    if (singleSlice) {
        docListReader.Open(singleSlice);
        _docListReader.Open(singleSlice);
    } else {
        docListReader.Open(postingList);
        _docListReader.Open(postingList);
    }

    TermMeta termMeta;
    TermMetaLoader tmLoader(mCurSegPostingFormatOption);
    tmLoader.Load(&docListReader, termMeta);
    uint32_t docSkipListSize = docListReader.ReadVUInt32();
    uint32_t docListSize = docListReader.ReadVUInt32();

    uint32_t docListBeginPos = docListReader.Tell() + docSkipListSize;
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _segmentDecoder);
    _segmentDecoder = CreateNormalSegmentDecoder(docCompressMode, docListBeginPos,
                                                 mCurSegPostingFormatOption.IsShortListVbyteCompress());

    uint32_t docSkipListStart = docListReader.Tell();
    uint32_t docSkipListEnd = docSkipListStart + docSkipListSize;
    if (singleSlice) {
        _segmentDecoder->InitSkipList(docSkipListStart, docSkipListEnd, singleSlice, termMeta.GetDocFreq(),
                                      mCurSegPostingFormatOption.GetDocListCompressMode());
    } else {
        _segmentDecoder->InitSkipList(docSkipListStart, docSkipListEnd, postingList, termMeta.GetDocFreq(),
                                      mCurSegPostingFormatOption.GetDocListCompressMode());
    }

    if (mCurSegPostingFormatOption.HasPositionList() || mCurSegPostingFormatOption.HasTfBitmap()) {
        uint8_t posCompressMode = ShortListOptimizeUtil::GetPosCompressMode(compressMode);
        uint32_t posListBegin = docListReader.Tell() + docSkipListSize + docListSize;

        // init tf_bitmap_reader & position_list
        if (singleSlice) {
            _inDocStateKeeper.MoveToSegment(singleSlice, termMeta.GetTotalTermFreq(), posListBegin, posCompressMode,
                                            mCurSegPostingFormatOption.GetPosListFormatOption());
        } else {
            _inDocStateKeeper.MoveToSegment(postingList, termMeta.GetTotalTermFreq(), posListBegin, posCompressMode,
                                            mCurSegPostingFormatOption.GetPosListFormatOption());
        }
    }

    if (mCurSegPostingFormatOption.HasPositionList()) {
        // InDocPositionIterator can't reentry in different segment
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _inDocPositionIterator);
        _inDocPositionIterator = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, NormalInDocPositionIterator,
                                                              mCurSegPostingFormatOption.GetPosListFormatOption());
    }

    ++_segmentCursor;
    return true;
}

BufferedSegmentIndexDecoder* BufferedIndexDecoder::CreateNormalSegmentDecoder(uint8_t compressMode,
                                                                              uint32_t docListBeginPos,
                                                                              bool enableShortListVbyteCompress)
{
    if (compressMode == SHORT_LIST_COMPRESS_MODE) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, ShortListSegmentDecoder, &_docListReader, docListBeginPos,
                                            compressMode, enableShortListVbyteCompress);
    }

    if (mCurSegPostingFormatOption.HasTfList()) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SkipListSegmentDecoder<TriValueSkipListReader>, _sessionPool,
                                            &_docListReader, docListBeginPos, compressMode);
    } else {
        return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SkipListSegmentDecoder<PairValueSkipListReader>, _sessionPool,
                                            &_docListReader, docListBeginPos, compressMode);
    }
}
} // namespace indexlib::index
