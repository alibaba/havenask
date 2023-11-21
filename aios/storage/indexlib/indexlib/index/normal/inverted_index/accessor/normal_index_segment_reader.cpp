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
#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"

#include "autil/TimeUtility.h"
#include "autil/memory.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/normal/util/segment_file_compress_util.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using future_lite::Try;
using future_lite::Unit;
using future_lite::coro::Lazy;

using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, NormalIndexSegmentReader);

NormalIndexSegmentReader::NormalIndexSegmentReader() : mBaseAddr(nullptr) {}

NormalIndexSegmentReader::~NormalIndexSegmentReader() {}

void NormalIndexSegmentReader::Open(const IndexConfigPtr& indexConfig, const DirectoryPtr& indexDirectory,
                                    const NormalIndexSegmentReader* hintReader)
{
    if (hintReader) {
        mDictReader = hintReader->mDictReader;
        mPostingReader = hintReader->mPostingReader;
        mBaseAddr = hintReader->mBaseAddr;
        mOption = hintReader->mOption;
        return;
    }
    string dictFilePath = DICTIONARY_FILE_NAME;
    string postingFilePath = POSTING_FILE_NAME;
    string formatFilePath = INDEX_FORMAT_OPTION_FILE_NAME;
    bool dictExist = indexDirectory->IsExist(dictFilePath);
    bool postingExist = indexDirectory->IsExist(postingFilePath);
    if (!dictExist && !postingExist) {
        return;
    }

    if (!dictExist || !postingExist) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "index[%s]:missing dictionary or posting file",
                             indexConfig->GetIndexName().c_str());
    }

    bool supportFileCompress = SegmentFileCompressUtil::IndexSupportFileCompress(indexConfig, mSegmentData);
    mDictReader.reset(CreateDictionaryReader(indexConfig));
    auto status = mDictReader->Open(indexDirectory, dictFilePath, supportFileCompress);
    THROW_IF_STATUS_ERROR(status);

    file_system::ReaderOption option(FSOT_LOAD_CONFIG);
    option.supportCompress = supportFileCompress;
    mPostingReader = indexDirectory->CreateFileReader(postingFilePath, option);
    mBaseAddr = (uint8_t*)mPostingReader->GetBaseAddress();
    if (indexDirectory->IsExist(formatFilePath)) {
        string indexFormatStr;
        indexDirectory->Load(formatFilePath, indexFormatStr);
        mOption = LegacyIndexFormatOption::FromString(indexFormatStr);
        mOption.SetNumberIndex(InvertedIndexUtil::IsNumberIndex(indexConfig));
    } else {
        mOption.Init(indexConfig);
        mOption.SetIsCompressedPostingHeader(false);
    }
}

DictionaryReader* NormalIndexSegmentReader::CreateDictionaryReader(const IndexConfigPtr& indexConfig) const
{
    return DictionaryCreator::CreateReader(indexConfig);
}

void NormalIndexSegmentReader::Open(const IndexConfigPtr& indexConfig, const index_base::SegmentData& segmentData,
                                    const NormalIndexSegmentReader* hintReader)
{
    assert(indexConfig);
    mSegmentData = segmentData;
    DirectoryPtr indexDirectory = segmentData.GetIndexDirectory(indexConfig->GetIndexName(), false);
    if (!indexDirectory) {
        IE_LOG(DEBUG, "[%s] not exist in segment [%d]", indexConfig->GetIndexName().c_str(),
               segmentData.GetSegmentId());
        return;
    }
    Open(indexConfig, indexDirectory, hintReader);
}

bool NormalIndexSegmentReader::GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId,
                                                 SegmentPosting& segPosting, Pool* sessionPool,
                                                 InvertedIndexSearchTracer* tracer) const
{
    file_system::ReadOption option;
    dictvalue_t dictValue;
    if (!mDictReader || !mDictReader->Lookup(key, option, dictValue).ValueOrThrow()) {
        return false;
    }
    InnerGetSegmentPosting(dictValue, segPosting, sessionPool);
    return true;
}

Lazy<index::Result<bool>>
NormalIndexSegmentReader::GetSegmentPostingAsync(const index::DictKeyInfo& key, docid_t baseDocId,
                                                 SegmentPosting& segPosting, Pool* sessionPool,
                                                 file_system::ReadOption option) const noexcept
{
    if (!mDictReader) {
        co_return false;
    }
    auto lookupResult = co_await mDictReader->LookupAsync(key, option);
    if (!lookupResult.Ok()) {
        co_return lookupResult.GetErrorCode();
    }
    if (!lookupResult.Value().first) {
        co_return false;
    }
    auto errorCode = co_await GetSegmentPostingAsync(lookupResult.Value().second, segPosting, sessionPool, option);
    if (errorCode != indexlib::index::ErrorCode::OK) {
        AUTIL_LOG(ERROR, "read segment posting fail, baseDocId[%d]", baseDocId);
        co_return errorCode;
    }
    co_return true;
}

void NormalIndexSegmentReader::InnerGetSegmentPosting(dictvalue_t dictValue, SegmentPosting& segPosting,
                                                      autil::mem_pool::Pool* sessionPool) const
{
    PostingFormatOption formatOption = mOption.GetPostingFormatOption();
    segPosting.SetPostingFormatOption(formatOption);

    bool isDocList = false;
    bool dfFirst = true;
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(dictValue, isDocList, dfFirst)) {
        segPosting.Init(mSegmentData.GetBaseDocId(), mSegmentData.GetSegmentInfo()->docCount, dictValue, isDocList,
                        dfFirst);
        return;
    }

    int64_t postingOffset = 0;
    ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);

    FileReaderPtr postingReader = GetSessionPostingFileReader(sessionPool);
    uint32_t postingLen;
    if (formatOption.IsCompressedPostingHeader()) {
        postingLen = postingReader->ReadVUInt32(postingOffset).GetOrThrow();
        postingOffset += VByteCompressor::GetVInt32Length(postingLen);
    } else {
        postingReader->Read(&postingLen, sizeof(uint32_t), postingOffset).GetOrThrow();
        postingOffset += sizeof(uint32_t);
    }

    if (mBaseAddr) {
        // no compress & integrated
        segPosting.Init(mBaseAddr + postingOffset, postingLen, mSegmentData.GetBaseDocId(),
                        mSegmentData.GetSegmentInfo()->docCount, dictValue);
    } else {
        ByteSliceList* sliceList = postingReader->ReadToByteSliceList(postingLen, postingOffset, ReadOption());
        if (unlikely(!sliceList)) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "ReadToByteSliceList failed, length[%u], offset[%ld]", postingLen,
                                 postingOffset);
        }
        segPosting.Init(ShortListOptimizeUtil::GetCompressMode(dictValue), ByteSliceListPtr(sliceList),
                        mSegmentData.GetBaseDocId(), mSegmentData.GetSegmentInfo()->docCount);
    }
    return;
}

file_system::FileReaderPtr
NormalIndexSegmentReader::GetSessionPostingFileReader(autil::mem_pool::Pool* sessionPool) const
{
    auto compressFileReader = DYNAMIC_POINTER_CAST(file_system::CompressFileReader, mPostingReader);
    if (compressFileReader) {
        return autil::shared_ptr_pool(sessionPool, compressFileReader->CreateSessionReader(sessionPool));
    }
    return mPostingReader;
}

Lazy<index::ErrorCode> NormalIndexSegmentReader::GetSegmentPostingAsync(dictvalue_t dictValue,
                                                                        SegmentPosting& segPosting,
                                                                        autil::mem_pool::Pool* sessionPool,
                                                                        file_system::ReadOption option) const noexcept
{
    bool isDocList = false;
    bool dfFirst = true;
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(dictValue, isDocList, dfFirst)) {
        PostingFormatOption formatOption = mOption.GetPostingFormatOption();
        segPosting.SetPostingFormatOption(formatOption);
        segPosting.Init(mSegmentData.GetBaseDocId(), mSegmentData.GetSegmentInfo()->docCount, dictValue, isDocList,
                        dfFirst);
        co_return index::ErrorCode::OK;
    }

    int64_t postingOffset = 0;
    ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);

    FileReaderPtr postingReader = GetSessionPostingFileReader(sessionPool);

    char buffer[8] = {0};
    uint32_t postingLen = 0;
    size_t readLen = min(postingReader->GetLogicLength() - postingOffset, sizeof(buffer));
    file_system::BatchIO batchIO;
    batchIO.emplace_back(buffer, readLen, postingOffset);
    auto result = co_await postingReader->BatchRead(batchIO, option);
    assert(1 == result.size());
    if (!result[0].OK()) {
        co_return index::ConvertFSErrorCode(result[0].Code());
    }
    if (mOption.GetPostingFormatOption().IsCompressedPostingHeader()) {
        char* bufferPtr = buffer;
        postingLen = VByteCompressor::ReadVUInt32(bufferPtr);
        postingOffset += VByteCompressor::GetVInt32Length(postingLen);
    } else {
        postingLen = *(uint32_t*)buffer;
        postingOffset += sizeof(uint32_t);
    }
    segPosting.SetPostingFormatOption(mOption.GetPostingFormatOption());
    if (mBaseAddr) {
        segPosting.Init(mBaseAddr + postingOffset, postingLen, mSegmentData.GetBaseDocId(),
                        mSegmentData.GetSegmentInfo()->docCount, dictValue);
    } else {
        // io
        auto sliceList = postingReader->ReadToByteSliceList(postingLen, postingOffset, ReadOption());
        if (unlikely(!sliceList)) {
            AUTIL_LOG(ERROR, "ReadToByteSliceList failed, length[%u], offset[%ld]", postingLen, postingOffset);
            co_return index::ErrorCode::FileIO;
        }
        if (!co_await sliceList->Prefetch(TermMetaDumper::MaxStoreSize())) {
            co_return index::ErrorCode::FileIO;
        }

        segPosting.Init(ShortListOptimizeUtil::GetCompressMode(dictValue), ByteSliceListPtr(sliceList),
                        mSegmentData.GetBaseDocId(), mSegmentData.GetSegmentInfo()->docCount);
    }
    co_return index::ErrorCode::OK;
}
}} // namespace indexlib::index
