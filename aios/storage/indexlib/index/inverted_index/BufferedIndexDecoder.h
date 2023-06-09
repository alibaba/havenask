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

#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/inverted_index/InDocStateKeeper.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/format/BufferedSegmentIndexDecoder.h"
#include "indexlib/index/inverted_index/format/NormalInDocPositionIterator.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"

namespace indexlib::index {

class BufferedIndexDecoder
{
public:
    inline BufferedIndexDecoder(NormalInDocState* state, autil::mem_pool::Pool* pool)
        : _baseDocId(0)
        , _needDecodeTF(false)
        , _needDecodeDocPayload(false)
        , _needDecodeFieldMap(false)
        , _segmentDecoder(NULL)
        , _segmentCursor(0)
        , _segmentCount(0)
        , _sessionPool(pool)
        , _inDocPositionIterator(NULL)
        , _inDocStateKeeper(state, pool)
    {
    }

    virtual ~BufferedIndexDecoder();

public:
    void Init(const std::shared_ptr<SegmentPostingVector>& segPostings);

public:
    virtual bool DecodeDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                                 ttf_t& currentTTF);

    virtual bool DecodeDocBufferMayCopy(docid_t startDocId, docid_t*& docBuffer, docid_t& firstDocId,
                                        docid_t& lastDocId, ttf_t& currentTTF);

    virtual bool DecodeCurrentTFBuffer(tf_t* tfBuffer);
    virtual void DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer);
    virtual void DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer);

    virtual uint32_t GetSeekedDocCount() const;
    uint32_t InnerGetSeekedDocCount() const { return _segmentDecoder->InnerGetSeekedDocCount(); }

    void MoveToCurrentDocPosition(ttf_t currentTTF);
    NormalInDocPositionIterator* GetPositionIterator();

    const PostingFormatOption& GetPostingFormatOption() const { return mCurSegPostingFormatOption; }
    docid_t GetCurrentSegmentBaseDocId() { return _baseDocId; }

private:
    bool DecodeDocBufferInOneSegment(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                                     ttf_t& currentTTF);
    bool DecodeDocBufferInOneSegmentMayCopy(docid_t startDocId, docid_t*& docBuffer, docid_t& firstDocId,
                                            docid_t& lastDocId, ttf_t& currentTTF);

    bool DecodeShortListDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                                  ttf_t& currentTTF);

    bool DecodeNormalListDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                                   ttf_t& currentTTF);

private:
    // virtual for test
    virtual BufferedSegmentIndexDecoder* CreateNormalSegmentDecoder(uint8_t compressMode, uint32_t docListBeginPos,
                                                                    bool enableShortListVbyteCompress);

private:
    bool MoveToSegment(docid_t startDocId);
    docid_t GetSegmentBaseDocId(uint32_t segCursor);
    uint32_t LocateSegment(uint32_t startSegCursor, docid_t startDocId);

protected:
    PostingFormatOption mCurSegPostingFormatOption;

private:
    docid_t _baseDocId;

    bool _needDecodeTF;
    bool _needDecodeDocPayload;
    bool _needDecodeFieldMap;

    BufferedSegmentIndexDecoder* _segmentDecoder;

    uint32_t _segmentCursor;
    uint32_t _segmentCount;
    std::shared_ptr<SegmentPostingVector> _segPostings;

    // PERF_OPT: reentry for multi segment decoder
    file_system::ByteSliceReader _docListReader;

    autil::mem_pool::Pool* _sessionPool;
    NormalInDocPositionIterator* _inDocPositionIterator;
    InDocStateKeeper _inDocStateKeeper;

private:
    friend class BufferedIndexDecoderTest;

private:
    AUTIL_LOG_DECLARE();
};

inline void BufferedIndexDecoder::MoveToCurrentDocPosition(ttf_t currentTTF)
{
    _inDocStateKeeper.MoveToDoc(currentTTF);
}

inline NormalInDocPositionIterator* BufferedIndexDecoder::GetPositionIterator() { return _inDocPositionIterator; }

inline docid_t BufferedIndexDecoder::GetSegmentBaseDocId(uint32_t segCursor)
{
    if (segCursor >= _segmentCount) {
        return INVALID_DOCID;
    }

    SegmentPosting& curSegPosting = (*_segPostings)[segCursor];
    return curSegPosting.GetBaseDocId();
}

inline uint32_t BufferedIndexDecoder::LocateSegment(uint32_t startSegCursor, docid_t startDocId)
{
    docid_t curSegBaseDocId = GetSegmentBaseDocId(startSegCursor);
    if (curSegBaseDocId == INVALID_DOCID) {
        return startSegCursor;
    }

    uint32_t curSegCursor = startSegCursor;
    docid_t nextSegBaseDocId = GetSegmentBaseDocId(curSegCursor + 1);
    while (nextSegBaseDocId != INVALID_DOCID && startDocId >= nextSegBaseDocId) {
        ++curSegCursor;
        nextSegBaseDocId = GetSegmentBaseDocId(curSegCursor + 1);
    }
    return curSegCursor;
}

inline uint32_t BufferedIndexDecoder::GetSeekedDocCount() const { return InnerGetSeekedDocCount(); }

typedef BufferedIndexDecoder BufferedPostingDecoder;
} // namespace indexlib::index
