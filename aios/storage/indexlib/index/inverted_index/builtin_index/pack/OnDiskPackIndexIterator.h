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
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/index/inverted_index/OnDiskIndexIteratorCreator.h"
#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/util/Bitmap.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {

#define DELETE_BYTE_SLICE(slice)                                                                                       \
    do {                                                                                                               \
        if (slice != nullptr) {                                                                                        \
            util::ByteSlice::DestroyObject(slice);                                                                     \
            slice = nullptr;                                                                                           \
        }                                                                                                              \
    } while (0)

template <typename DictKey>
class OnDiskPackIndexIteratorTyped : public OnDiskIndexIterator
{
private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 1024 * 1024 * 2; // 2M

public:
    OnDiskPackIndexIteratorTyped(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                 const std::shared_ptr<file_system::Directory>& indexDirectory,
                                 const PostingFormatOption& postingFormatOption,
                                 const file_system::IOConfig& ioConfig = file_system::IOConfig());

    virtual ~OnDiskPackIndexIteratorTyped();

    class Creator : public OnDiskIndexIteratorCreator
    {
    public:
        Creator(const PostingFormatOption& postingFormatOption, const file_system::IOConfig& ioConfig,
                const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
            : _postingFormatOption(postingFormatOption)
            , _ioConfig(ioConfig)
            , _indexConfig(indexConfig)
        {
        }
        OnDiskIndexIterator* Create(const std::shared_ptr<file_system::Directory>& segDir) const override
        {
            auto subDir = segDir->GetDirectory(INVERTED_INDEX_PATH, /*throwExceptionIfNotExist=*/false);
            if (!subDir) {
                return nullptr;
            }
            auto indexDirectory =
                subDir->GetDirectory(_indexConfig->GetIndexName(), /*throwExceptionIfNotExist=*/false);
            return CreateByIndexDir(indexDirectory);
        }
        OnDiskIndexIterator*
        CreateByIndexDir(const std::shared_ptr<file_system::Directory>& indexDirectory) const override
        {
            if (indexDirectory && indexDirectory->IsExist(DICTIONARY_FILE_NAME)) {
                return (new OnDiskPackIndexIteratorTyped<DictKey>(_indexConfig, indexDirectory, _postingFormatOption,
                                                                  _ioConfig));
            }
            return nullptr;
        }

    private:
        PostingFormatOption _postingFormatOption;
        file_system::IOConfig _ioConfig;
        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    };

public:
    void Init() override;
    bool HasNext() const override;

    PostingDecoder* Next(index::DictKeyInfo& key) override;
    size_t GetPostingFileLength() const override { return _postingFile ? _postingFile->GetLogicLength() : 0; }

private:
    virtual void CreatePostingDecoder() { _decoder.reset(new PostingDecoderImpl(this->_postingFormatOption)); }

private:
    void DecodeTermMeta();
    void DecodeDocList();
    void DecodeTfBitmap();
    void DecodePosList();

    void InitDecoderForDictInlinePosting(dictvalue_t value, bool isDocList, bool dfFirst);
    void InitDecoderForNormalPosting(dictvalue_t value);

protected:
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::shared_ptr<DictionaryIterator> _dictionaryIterator;
    std::shared_ptr<file_system::FileReader> _postingFile;

    util::ByteSlice* _docListSlice = nullptr;
    util::ByteSlice* _posListSlice = nullptr;

    std::shared_ptr<file_system::ByteSliceReader> _docListReader;
    std::shared_ptr<file_system::ByteSliceReader> _posListReader;
    std::shared_ptr<util::Bitmap> _tfBitmap;

    TermMeta* _termMeta = nullptr;
    std::shared_ptr<PostingDecoderImpl> _decoder;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, OnDiskPackIndexIteratorTyped, DictKey);
typedef OnDiskPackIndexIteratorTyped<dictkey_t> OnDiskPackIndexIterator;

template <typename DictKey>
OnDiskPackIndexIteratorTyped<DictKey>::OnDiskPackIndexIteratorTyped(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
    const std::shared_ptr<file_system::Directory>& indexDirectory, const PostingFormatOption& postingFormatOption,
    const file_system::IOConfig& ioConfig)
    : OnDiskIndexIterator(indexDirectory, postingFormatOption, ioConfig)
    , _indexConfig(indexConfig)
    , _docListSlice(nullptr)
    , _posListSlice(nullptr)
    , _termMeta(nullptr)
{
}

template <typename DictKey>
OnDiskPackIndexIteratorTyped<DictKey>::~OnDiskPackIndexIteratorTyped()
{
    DELETE_BYTE_SLICE(_docListSlice);
    DELETE_BYTE_SLICE(_posListSlice);
    DELETE_AND_SET_NULL(_termMeta);
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::Init()
{
    if (_indexDirectory->IsExist(INDEX_FORMAT_OPTION_FILE_NAME)) {
        std::string indexFormatStr;
        _indexDirectory->Load(INDEX_FORMAT_OPTION_FILE_NAME, indexFormatStr);
        IndexFormatOption option = IndexFormatOption::FromString(indexFormatStr);
        _postingFormatOption = option.GetPostingFormatOption();
    } else {
        _postingFormatOption.SetIsCompressedPostingHeader(false);
    }

    auto compressConfig = _indexConfig->GetFileCompressConfig();
    bool supportCompress = true;
    std::shared_ptr<DictionaryReader> dictionaryReader(DictionaryCreator::CreateDiskReader(_indexConfig));
    auto status = dictionaryReader->Open(_indexDirectory, DICTIONARY_FILE_NAME, supportCompress);
    THROW_IF_STATUS_ERROR(status);
    _dictionaryIterator = dictionaryReader->CreateIterator();

    file_system::ReaderOption option(file_system::FSOT_BUFFERED);
    option.supportCompress = supportCompress;
    _postingFile = _indexDirectory->CreateFileReader(POSTING_FILE_NAME, option);
    assert(_postingFile);
    // _postingFile->ResetBufferParam(this->mIOConfig.readBufferSize, this->mIOConfig.enableAsyncRead);

    _docListReader.reset(new file_system::ByteSliceReader());
    if (this->_postingFormatOption.HasTfBitmap()) {
        _tfBitmap.reset(new util::Bitmap());
    }
    if (this->_postingFormatOption.HasPositionList()) {
        _posListReader.reset(new file_system::ByteSliceReader());
    }

    _termMeta = new TermMeta();
    CreatePostingDecoder();
}

template <typename DictKey>
bool OnDiskPackIndexIteratorTyped<DictKey>::HasNext() const
{
    return _dictionaryIterator->HasNext();
}

template <typename DictKey>
PostingDecoder* OnDiskPackIndexIteratorTyped<DictKey>::Next(index::DictKeyInfo& key)
{
    dictvalue_t value;
    _dictionaryIterator->Next(key, value);
    bool isDocList = false;
    bool dfFirst = true;
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(value, isDocList, dfFirst)) {
        InitDecoderForDictInlinePosting(value, isDocList, dfFirst);
    } else {
        InitDecoderForNormalPosting(value);
    }

    if (!HasNext()) {
        // not close posting file in destruct
        _postingFile->Close().GetOrThrow();
    }
    return _decoder.get();
}

template <typename DictKey>
inline void OnDiskPackIndexIteratorTyped<DictKey>::InitDecoderForDictInlinePosting(dictvalue_t value, bool isDocList,
                                                                                   bool dfFirst)
{
    _decoder->Init(_termMeta, value, isDocList, dfFirst);
}

template <typename DictKey>
inline void OnDiskPackIndexIteratorTyped<DictKey>::InitDecoderForNormalPosting(dictvalue_t value)
{
    int64_t fileCursor = 0;
    ShortListOptimizeUtil::GetOffset(value, fileCursor);

    uint32_t totalLen = 0;
    _postingFile->Seek(fileCursor).GetOrThrow();
    if (_postingFormatOption.IsCompressedPostingHeader()) {
        totalLen = _postingFile->ReadVUInt32().GetOrThrow();
        totalLen += VByteCompressor::GetVInt32Length(totalLen);
    } else {
        _postingFile->Read((void*)(&totalLen), sizeof(uint32_t)).GetOrThrow();
        totalLen += sizeof(uint32_t);
    }

    DecodeTermMeta();
    DecodeDocList();

    if (this->_postingFormatOption.HasTfBitmap()) {
        DecodeTfBitmap();
    }

    if (this->_postingFormatOption.HasPositionList()) {
        DecodePosList();
    }

    auto docListReader = std::dynamic_pointer_cast<file_system::ByteSliceReader>(_docListReader);
    auto posListReader = std::dynamic_pointer_cast<file_system::ByteSliceReader>(_posListReader);
    _decoder->Init(_termMeta, docListReader, posListReader, _tfBitmap, totalLen);
}

template <typename DictKey>
inline void OnDiskPackIndexIteratorTyped<DictKey>::DecodeTermMeta()
{
    TermMetaLoader tmLoader(_postingFormatOption);
    tmLoader.Load(_postingFile, *_termMeta);
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::DecodeDocList()
{
    uint32_t docSkipListLen = _postingFile->ReadVUInt32().GetOrThrow();
    uint32_t docListLen = _postingFile->ReadVUInt32().GetOrThrow();

    AUTIL_LOG(TRACE1, "doc skip list length: [%u], doc list length: [%u]", docSkipListLen, docListLen);

    DELETE_BYTE_SLICE(_docListSlice);
    _docListSlice = util::ByteSlice::CreateObject(docListLen);

    int64_t cursor = _postingFile->Tell() + docSkipListLen;
    ssize_t readLen = _postingFile->Read(_docListSlice->data, _docListSlice->size, cursor).GetOrThrow();
    assert(readLen == (ssize_t)docListLen);
    (void)readLen;
    _docListReader->Open(_docListSlice).GetOrThrow();
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::DecodeTfBitmap()
{
    uint32_t bitmapBlockCount = _postingFile->ReadVUInt32().GetOrThrow();
    uint32_t totalPosCount = _postingFile->ReadVUInt32().GetOrThrow();

    int64_t cursor = _postingFile->Tell();
    cursor += bitmapBlockCount * sizeof(uint32_t);
    uint32_t bitmapSizeInByte = util::Bitmap::GetDumpSize(totalPosCount);
    uint8_t* bitmapData = new uint8_t[bitmapSizeInByte];
    _postingFile->Read((void*)bitmapData, bitmapSizeInByte, cursor).GetOrThrow();

    _tfBitmap.reset(new util::Bitmap());
    _tfBitmap->MountWithoutRefreshSetCount(totalPosCount, (uint32_t*)bitmapData, false);
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::DecodePosList()
{
    uint32_t posSkipListLen = _postingFile->ReadVUInt32().GetOrThrow();
    uint32_t posListLen = _postingFile->ReadVUInt32().GetOrThrow();

    DELETE_BYTE_SLICE(_posListSlice);
    _posListSlice = util::ByteSlice::CreateObject(posListLen);

    int64_t cursor = _postingFile->Tell() + posSkipListLen;
    _postingFile->Read(_posListSlice->data, _posListSlice->size, cursor).GetOrThrow();
    _posListReader->Open(_posListSlice).GetOrThrow();
}

#undef DELETE_BYTE_SLICE

} // namespace indexlib::index
