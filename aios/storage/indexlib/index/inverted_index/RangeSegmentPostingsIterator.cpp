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
#include "indexlib/index/inverted_index/RangeSegmentPostingsIterator.h"

#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/format/DictInlinePostingDecoder.h"
#include "indexlib/index/inverted_index/format/InMemPostingDecoder.h"
#include "indexlib/index/inverted_index/format/ShortListSegmentDecoder.h"
#include "indexlib/index/inverted_index/format/SkipListSegmentDecoder.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"
#include "indexlib/index/inverted_index/format/skiplist/PairValueSkipListReader.h"
#include "indexlib/index/inverted_index/format/skiplist/TriValueSkipListReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, RangeSegmentPostingsIterator);

RangeSegmentPostingsIterator::RangeSegmentPostingsIterator(const PostingFormatOption& postingFormatOption,
                                                           uint32_t& seekDocCounter, autil::mem_pool::Pool* sessionPool)
    : _sessionPool(sessionPool)
    , _seekDocCounter(seekDocCounter)
    , _docBuffer(NULL)
    , _currentDocId(INVALID_DOCID)
    , _baseDocId(INVALID_DOCID)
    , _nextSegmentDocId(INVALID_DOCID)
    , _bufferLength(0)
{
}

void RangeSegmentPostingsIterator::Reset()
{
    while (!_heap.empty()) {
        _heap.pop();
    }
    _currentDocId = INVALID_DOCID;
    _baseDocId = INVALID_DOCID;
    _nextSegmentDocId = INVALID_DOCID;
    for (size_t i = 0; i < _segmentDecoders.size(); i++) {
        if (_segmentDecoders[i]) {
            IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _segmentDecoders[i]);
        }
    }
    IE_POOL_COMPATIBLE_DELETE_VECTOR(_sessionPool, _docBuffer, MAX_DOC_PER_RECORD * _bufferLength);
    _bufferLength = 0;
    _docListReaders.clear();
    _segmentDecoders.clear();
    _segPostings.reset();
}

RangeSegmentPostingsIterator::~RangeSegmentPostingsIterator()
{
    for (size_t i = 0; i < _segmentDecoders.size(); i++) {
        if (_segmentDecoders[i]) {
            IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _segmentDecoders[i]);
        }
    }
    IE_POOL_COMPATIBLE_DELETE_VECTOR(_sessionPool, _docBuffer, MAX_DOC_PER_RECORD * _bufferLength);
}

void RangeSegmentPostingsIterator::Init(const std::shared_ptr<SegmentPostings>& segPostings, docid_t startDocId,
                                        docid_t nextSegmentDocId)
{
    std::vector<SegmentPosting>& postingVec = segPostings->GetSegmentPostings();
    size_t bufferLength = postingVec.size();
    bufferLength = std::max(bufferLength, (size_t)100);
    if (_docBuffer && bufferLength > _bufferLength) {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_sessionPool, _docBuffer, MAX_DOC_PER_RECORD * _bufferLength);
        _docBuffer = NULL;
    }
    if (!_docBuffer) {
        _docBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_sessionPool, docid_t, MAX_DOC_PER_RECORD * bufferLength);
    }
    _bufferLength = bufferLength;
    _segPostings = segPostings;
    while (!_heap.empty()) {
        _heap.pop();
    }

    for (size_t i = 0; i < _segmentDecoders.size(); i++) {
        if (_segmentDecoders[i]) {
            IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _segmentDecoders[i]);
        }
    }

    _docListReaders.resize(postingVec.size());
    _segmentDecoders.resize(postingVec.size(), NULL);
    _baseDocId = _segPostings->GetBaseDocId();
    _nextSegmentDocId = nextSegmentDocId;
    docid_t curSegDocId = std::max(docid_t(0), startDocId - _baseDocId);
    ttf_t currentTTF = 0;
    docid_t firstDocId = INVALID_DOCID;
    docid_t lastDocId = INVALID_DOCID;
    std::vector<PostingItem> postingItems;
    postingItems.reserve(postingVec.size());
    for (size_t i = 0; i < postingVec.size(); i++) {
        _segmentDecoders[i] = CreateSegmentDecoder(postingVec[i], &_docListReaders[i]);
        if (!_segmentDecoders[i]->DecodeDocBuffer(curSegDocId, _docBuffer + i * MAX_DOC_PER_RECORD, firstDocId,
                                                  lastDocId, currentTTF)) {
            continue;
        }
        postingItems.emplace_back(firstDocId, lastDocId, i * MAX_DOC_PER_RECORD);
    }
    _heap.makeHeap(postingItems.begin(), postingItems.end());
}

BufferedSegmentIndexDecoder*
RangeSegmentPostingsIterator::CreateSegmentDecoder(const SegmentPosting& curSegPosting,
                                                   file_system::ByteSliceReader* docListReaderPtr)
{
    PostingFormatOption curSegPostingFormatOption = curSegPosting.GetPostingFormatOption();
    uint8_t compressMode = curSegPosting.GetCompressMode();
    uint8_t docCompressMode = ShortListOptimizeUtil::GetDocCompressMode(compressMode);
    BufferedSegmentIndexDecoder* segmentDecoder = NULL;
    if (docCompressMode == DICT_INLINE_COMPRESS_MODE) {
        segmentDecoder = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, DictInlinePostingDecoder, curSegPostingFormatOption,
                                                      curSegPosting.GetDictInlinePostingData(),
                                                      curSegPosting.IsDocListDictInline(), curSegPosting.IsDfFirst());
        return segmentDecoder;
    }

    const PostingWriter* postingWriter = curSegPosting.GetInMemPostingWriter();
    if (postingWriter) {
        InMemPostingDecoder* postingDecoder = postingWriter->CreateInMemPostingDecoder(_sessionPool);
        segmentDecoder = postingDecoder->GetInMemDocListDecoder();
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, postingDecoder);
        return segmentDecoder;
    }

    file_system::ByteSliceReader docListReader;
    util::ByteSlice* singleSlice = curSegPosting.GetSingleSlice();
    util::ByteSliceList* postingList = curSegPosting.GetSliceListPtr().get();
    if (singleSlice) {
        docListReader.Open(singleSlice);
        docListReaderPtr->Open(singleSlice);
    } else {
        docListReader.Open(postingList);
        // do not use doclist reader to read, because reader can't seek back.
        docListReaderPtr->Open(postingList);
    }

    TermMeta termMeta;
    TermMetaLoader tmLoader(curSegPostingFormatOption);
    tmLoader.Load(&docListReader, termMeta);
    uint32_t docSkipListSize = docListReader.ReadVUInt32();
    docListReader.ReadVUInt32(); // move doc list size
    uint32_t docListBeginPos = docListReader.Tell() + docSkipListSize;
    segmentDecoder =
        CreateNormalSegmentDecoder(docListReaderPtr, docCompressMode, docListBeginPos, curSegPostingFormatOption);

    uint32_t docSkipListStart = docListReader.Tell();
    uint32_t docSkipListEnd = docSkipListStart + docSkipListSize;

    if (singleSlice) {
        segmentDecoder->InitSkipList(docSkipListStart, docSkipListEnd, singleSlice, termMeta.GetDocFreq(),
                                     curSegPostingFormatOption.GetDocListCompressMode());
    } else {
        segmentDecoder->InitSkipList(docSkipListStart, docSkipListEnd, postingList, termMeta.GetDocFreq(),
                                     curSegPostingFormatOption.GetDocListCompressMode());
    }
    return segmentDecoder;
}

BufferedSegmentIndexDecoder*
RangeSegmentPostingsIterator::CreateNormalSegmentDecoder(file_system::ByteSliceReader* docListReader,
                                                         uint8_t compressMode, uint32_t docListBeginPos,
                                                         PostingFormatOption& curSegPostingFormatOption)
{
    if (compressMode == SHORT_LIST_COMPRESS_MODE) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, ShortListSegmentDecoder, docListReader, docListBeginPos,
                                            compressMode, curSegPostingFormatOption.IsShortListVbyteCompress());
    }

    if (curSegPostingFormatOption.HasTfList()) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SkipListSegmentDecoder<TriValueSkipListReader>, _sessionPool,
                                            docListReader, docListBeginPos, compressMode);
    } else {
        return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SkipListSegmentDecoder<PairValueSkipListReader>, _sessionPool,
                                            docListReader, docListBeginPos, compressMode);
    }
}
} // namespace indexlib::index
