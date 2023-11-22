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
#include "indexlib/index/inverted_index/InvertedLeafReader.h"

#include "autil/memory.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedLeafReader);

InvertedLeafReader::InvertedLeafReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                       const IndexFormatOption& indexFormatOption, uint64_t docCount,
                                       const std::shared_ptr<DictionaryReader>& dictReader,
                                       const std::shared_ptr<file_system::FileReader>& postingReader)
    : _dictReader(dictReader)
    , _indexConfig(indexConfig)
    , _indexFormatOption(indexFormatOption)
    , _docCount(docCount)

    , _postingReader(postingReader)
{
    if (_postingReader) {
        _baseAddr = (uint8_t*)_postingReader->GetBaseAddress();
    }
}

bool InvertedLeafReader::GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId,
                                           SegmentPosting& segPosting, autil::mem_pool::Pool* sessionPool,
                                           file_system::ReadOption option, InvertedIndexSearchTracer* tracer) const
{
    dictvalue_t dictValue;
    if (!_dictReader) {
        return false;
    }
    if (tracer) {
        tracer->IncDictionaryLookupCount();
        option.blockCounter = tracer->GetDictionaryBlockCacheCounter();
    }
    if (!_dictReader->Lookup(key, option, dictValue).ValueOrThrow()) {
        return false;
    }
    if (tracer) {
        tracer->IncDictionaryHitCount();
    }
    InnerGetSegmentPosting(dictValue, baseDocId, segPosting, sessionPool);
    return true;
}

IndexFormatOption InvertedLeafReader::GetIndexFormatOption() const { return _indexFormatOption; }
std::shared_ptr<indexlibv2::config::InvertedIndexConfig> InvertedLeafReader::GetIndexConfig() const
{
    return _indexConfig;
}
std::shared_ptr<file_system::FileReader> InvertedLeafReader::GetPostingReader() const { return _postingReader; }

void InvertedLeafReader::InnerGetSegmentPosting(dictvalue_t dictValue, docid64_t baseDocId, SegmentPosting& segPosting,
                                                autil::mem_pool::Pool* sessionPool) const
{
    PostingFormatOption postingFormatOption = _indexFormatOption.GetPostingFormatOption();
    segPosting.SetPostingFormatOption(postingFormatOption);

    bool isDocList = false;
    bool dfFirst = true;
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(dictValue, isDocList, dfFirst)) {
        segPosting.Init(baseDocId, _docCount, dictValue, isDocList, dfFirst);
        return;
    }

    int64_t postingOffset = 0;
    ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);

    std::shared_ptr<file_system::FileReader> postingReader = GetSessionPostingFileReader(sessionPool);
    uint32_t postingLen;
    if (postingFormatOption.IsCompressedPostingHeader()) {
        postingLen = postingReader->ReadVUInt32(postingOffset).GetOrThrow();
        postingOffset += VByteCompressor::GetVInt32Length(postingLen);
    } else {
        postingReader->Read(&postingLen, sizeof(uint32_t), postingOffset).GetOrThrow();
        postingOffset += sizeof(uint32_t);
    }

    if (_baseAddr) {
        // no compress & integrated
        segPosting.Init(_baseAddr + postingOffset, postingLen, baseDocId, _docCount, dictValue);
    } else {
        util::ByteSliceList* sliceList =
            postingReader->ReadToByteSliceList(postingLen, postingOffset, file_system::ReadOption());
        if (unlikely(!sliceList)) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "ReadToByteSliceList failed, length[%u], offset[%ld]", postingLen,
                                 postingOffset);
        }
        segPosting.Init(ShortListOptimizeUtil::GetCompressMode(dictValue),
                        std::shared_ptr<util::ByteSliceList>(sliceList), baseDocId, _docCount);
    }
    return;
}

std::shared_ptr<file_system::FileReader>
InvertedLeafReader::GetSessionPostingFileReader(autil::mem_pool::Pool* sessionPool) const
{
    auto compressFileReader = std::dynamic_pointer_cast<file_system::CompressFileReader>(_postingReader);
    if (compressFileReader) {
        return autil::shared_ptr_pool(sessionPool, compressFileReader->CreateSessionReader(sessionPool));
    }
    return _postingReader;
}

future_lite::coro::Lazy<index::Result<bool>> InvertedLeafReader::GetSegmentPostingAsync(
    const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting, autil::mem_pool::Pool* sessionPool,
    file_system::ReadOption option, InvertedIndexSearchTracer* tracer) const noexcept
{
    if (!_dictReader) {
        co_return false;
    }
    if (tracer) {
        tracer->IncDictionaryLookupCount();
        option.blockCounter = tracer->GetDictionaryBlockCacheCounter();
    }
    auto lookupResult = co_await _dictReader->LookupAsync(key, option);
    if (!lookupResult.Ok()) {
        co_return lookupResult.GetErrorCode();
    }
    if (!lookupResult.Value().first) {
        co_return false;
    }
    if (tracer) {
        tracer->IncDictionaryHitCount();
    }
    auto errorCode = co_await GetSegmentPostingAsync(lookupResult.Value().second, baseDocId, segPosting, sessionPool,
                                                     option, tracer);
    if (errorCode != index::ErrorCode::OK) {
        AUTIL_LOG(ERROR, "read segment posting fail, baseDocId[%ld]", baseDocId);
        co_return errorCode;
    }
    co_return true;
}

future_lite::coro::Lazy<index::ErrorCode>
InvertedLeafReader::GetSegmentPostingAsync(dictvalue_t dictValue, docid64_t baseDocId, SegmentPosting& segPosting,
                                           autil::mem_pool::Pool* sessionPool, file_system::ReadOption option,
                                           InvertedIndexSearchTracer* tracer) const noexcept
{
    bool isDocList = false;
    bool dfFirst = true;
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(dictValue, isDocList, dfFirst)) {
        PostingFormatOption formatOption = _indexFormatOption.GetPostingFormatOption();
        segPosting.SetPostingFormatOption(formatOption);
        segPosting.Init(baseDocId, _docCount, dictValue, isDocList, dfFirst);
        co_return index::ErrorCode::OK;
    }

    int64_t postingOffset = 0;
    ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);

    std::shared_ptr<file_system::FileReader> postingReader = GetSessionPostingFileReader(sessionPool);

    char buffer[8] = {0};
    uint32_t postingLen = 0;
    size_t readLen = std::min(postingReader->GetLogicLength() - postingOffset, sizeof(buffer));
    file_system::BatchIO batchIO;
    batchIO.emplace_back(buffer, readLen, postingOffset);
    if (tracer) {
        option.blockCounter = tracer->GetPostingBlockCacheCounter();
    }
    auto result = co_await postingReader->BatchRead(batchIO, option);
    assert(1 == result.size());
    if (!result[0].OK()) {
        co_return index::ConvertFSErrorCode(result[0].Code());
    }
    if (_indexFormatOption.GetPostingFormatOption().IsCompressedPostingHeader()) {
        char* bufferPtr = buffer;
        postingLen = VByteCompressor::ReadVUInt32(bufferPtr);
        postingOffset += VByteCompressor::GetVInt32Length(postingLen);
    } else {
        postingLen = *(uint32_t*)buffer;
        postingOffset += sizeof(uint32_t);
    }
    segPosting.SetPostingFormatOption(_indexFormatOption.GetPostingFormatOption());
    if (_baseAddr) {
        segPosting.Init(_baseAddr + postingOffset, postingLen, baseDocId, _docCount, dictValue);
    } else {
        // io
        auto sliceList = postingReader->ReadToByteSliceList(postingLen, postingOffset, option);
        if (unlikely(!sliceList)) {
            AUTIL_LOG(ERROR, "ReadToByteSliceList failed, length[%u], offset[%ld]", postingLen, postingOffset);
            co_return index::ErrorCode::FileIO;
        }
        if (!co_await sliceList->Prefetch(TermMetaDumper::MaxStoreSize())) {
            co_return index::ErrorCode::FileIO;
        }
        segPosting.Init(ShortListOptimizeUtil::GetCompressMode(dictValue),
                        std::shared_ptr<util::ByteSliceList>(sliceList), baseDocId, _docCount);
    }
    co_return index::ErrorCode::OK;
}

size_t InvertedLeafReader::EvaluateCurrentMemUsed() const
{
    size_t totalMemUse = sizeof(*this);
    if (_postingReader && _postingReader->IsMemLock()) {
        totalMemUse += _postingReader->GetLength();
    }
    if (_dictReader) {
        totalMemUse += _dictReader->EstimateMemUsed();
    }
    return totalMemUse;
}

} // namespace indexlib::index
