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
#ifndef __INDEXLIB_ON_DISK_PACK_INDEX_ITERATOR_H
#define __INDEXLIB_ON_DISK_PACK_INDEX_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Bitmap.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib { namespace index { namespace legacy {

#define DELETE_BYTE_SLICE(slice)                                                                                       \
    do {                                                                                                               \
        if (slice != NULL) {                                                                                           \
            util::ByteSlice::DestroyObject(slice);                                                                     \
            slice = NULL;                                                                                              \
        }                                                                                                              \
    } while (0)

template <typename DictKey>
class OnDiskPackIndexIteratorTyped : public OnDiskIndexIterator
{
private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 1024 * 1024 * 2; // 2M

public:
    OnDiskPackIndexIteratorTyped(const config::IndexConfigPtr& indexConfig,
                                 const file_system::DirectoryPtr& indexDirectory,
                                 const PostingFormatOption& postingFormatOption,
                                 const config::MergeIOConfig& ioConfig = config::MergeIOConfig());

    virtual ~OnDiskPackIndexIteratorTyped();

    class Creator : public OnDiskIndexIteratorCreator
    {
    public:
        Creator(const PostingFormatOption postingFormatOption, const config::MergeIOConfig& ioConfig,
                const config::IndexConfigPtr& indexConfig)
            : mPostingFormatOption(postingFormatOption)
            , mIOConfig(ioConfig)
            , mIndexConfig(indexConfig)
        {
        }
        OnDiskIndexIterator* Create(const index_base::SegmentData& segData) const
        {
            file_system::DirectoryPtr indexDirectory = segData.GetIndexDirectory(mIndexConfig->GetIndexName(), false);
            if (indexDirectory && indexDirectory->IsExist(DICTIONARY_FILE_NAME)) {
                return (new OnDiskPackIndexIteratorTyped<DictKey>(mIndexConfig, indexDirectory, mPostingFormatOption,
                                                                  mIOConfig));
            }
            return NULL;
        }

    private:
        PostingFormatOption mPostingFormatOption;
        config::MergeIOConfig mIOConfig;
        config::IndexConfigPtr mIndexConfig;
    };

public:
    void Init() override;
    bool HasNext() const override;

    PostingDecoder* Next(index::DictKeyInfo& key) override;
    size_t GetPostingFileLength() const override { return mPostingFile ? mPostingFile->GetLogicLength() : 0; }

private:
    virtual void CreatePostingDecoder() { mDecoder.reset(new PostingDecoderImpl(this->_postingFormatOption)); }

private:
    void DecodeTermMeta();
    void DecodeDocList();
    void DecodeTfBitmap();
    void DecodePosList();

    void InitDecoderForDictInlinePosting(dictvalue_t value, bool isDocList, bool dfFirst);
    void InitDecoderForNormalPosting(dictvalue_t value);

protected:
    config::IndexConfigPtr mIndexConfig;
    std::shared_ptr<DictionaryIterator> mDictionaryIterator;
    file_system::FileReaderPtr mPostingFile;

    util::ByteSlice* mDocListSlice;
    util::ByteSlice* mPosListSlice;

    file_system::ByteSliceReaderPtr mDocListReader;
    file_system::ByteSliceReaderPtr mPosListReader;
    util::BitmapPtr mTfBitmap;

    TermMeta* mTermMeta;
    std::shared_ptr<PostingDecoderImpl> mDecoder;

private:
    friend class OnDiskIndexIteratorTest;
    friend class OnDiskPackIndexIteratorTest;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, OnDiskPackIndexIteratorTyped);
typedef OnDiskPackIndexIteratorTyped<dictkey_t> OnDiskPackIndexIterator;
DEFINE_SHARED_PTR(OnDiskPackIndexIterator);

template <typename DictKey>
OnDiskPackIndexIteratorTyped<DictKey>::OnDiskPackIndexIteratorTyped(const config::IndexConfigPtr& indexConfig,
                                                                    const file_system::DirectoryPtr& indexDirectory,
                                                                    const PostingFormatOption& postingFormatOption,
                                                                    const config::MergeIOConfig& ioConfig)
    : OnDiskIndexIterator(indexDirectory, postingFormatOption, ioConfig)
    , mIndexConfig(indexConfig)
    , mDocListSlice(NULL)
    , mPosListSlice(NULL)
    , mTermMeta(NULL)
{
}

template <typename DictKey>
OnDiskPackIndexIteratorTyped<DictKey>::~OnDiskPackIndexIteratorTyped()
{
    DELETE_BYTE_SLICE(mDocListSlice);
    DELETE_BYTE_SLICE(mPosListSlice);
    DELETE_AND_SET_NULL(mTermMeta);
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::Init()
{
    if (_indexDirectory->IsExist(INDEX_FORMAT_OPTION_FILE_NAME)) {
        std::string indexFormatStr;
        _indexDirectory->Load(INDEX_FORMAT_OPTION_FILE_NAME, indexFormatStr);
        LegacyIndexFormatOption option = LegacyIndexFormatOption::FromString(indexFormatStr);
        _postingFormatOption = option.GetPostingFormatOption();
    } else {
        _postingFormatOption.SetIsCompressedPostingHeader(false);
    }

    auto compressConfig = mIndexConfig->GetFileCompressConfig();
    std::shared_ptr<DictionaryReader> dictionaryReader(DictionaryCreator::CreateDiskReader(mIndexConfig));
    auto status = dictionaryReader->Open(_indexDirectory, DICTIONARY_FILE_NAME, compressConfig != nullptr);
    THROW_IF_STATUS_ERROR(status);
    mDictionaryIterator = dictionaryReader->CreateIterator();

    file_system::ReaderOption option(file_system::FSOT_BUFFERED);
    option.supportCompress = (compressConfig != nullptr);
    mPostingFile = _indexDirectory->CreateFileReader(POSTING_FILE_NAME, option);
    assert(mPostingFile);
    // mPostingFile->ResetBufferParam(this->mIOConfig.readBufferSize, this->mIOConfig.enableAsyncRead);

    mDocListReader.reset(new file_system::ByteSliceReader());
    if (this->_postingFormatOption.HasTfBitmap()) {
        mTfBitmap.reset(new util::Bitmap());
    }
    if (this->_postingFormatOption.HasPositionList()) {
        mPosListReader.reset(new file_system::ByteSliceReader());
    }

    mTermMeta = new TermMeta();
    CreatePostingDecoder();
}

template <typename DictKey>
bool OnDiskPackIndexIteratorTyped<DictKey>::HasNext() const
{
    return mDictionaryIterator->HasNext();
}

template <typename DictKey>
PostingDecoder* OnDiskPackIndexIteratorTyped<DictKey>::Next(index::DictKeyInfo& key)
{
    dictvalue_t value;
    mDictionaryIterator->Next(key, value);
    bool isDocList = false;
    bool dfFirst = true;
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(value, isDocList, dfFirst)) {
        InitDecoderForDictInlinePosting(value, isDocList, dfFirst);
    } else {
        InitDecoderForNormalPosting(value);
    }

    if (!HasNext()) {
        // not close posting file in destruct
        mPostingFile->Close().GetOrThrow();
    }
    return mDecoder.get();
}

template <typename DictKey>
inline void OnDiskPackIndexIteratorTyped<DictKey>::InitDecoderForDictInlinePosting(dictvalue_t value, bool isDocList,
                                                                                   bool dfFirst)
{
    mDecoder->Init(mTermMeta, value, isDocList, dfFirst);
}

template <typename DictKey>
inline void OnDiskPackIndexIteratorTyped<DictKey>::InitDecoderForNormalPosting(dictvalue_t value)
{
    int64_t fileCursor = 0;
    ShortListOptimizeUtil::GetOffset(value, fileCursor);

    uint32_t totalLen = 0;
    mPostingFile->Seek(fileCursor).GetOrThrow();
    if (_postingFormatOption.IsCompressedPostingHeader()) {
        totalLen = mPostingFile->ReadVUInt32().GetOrThrow();
        totalLen += VByteCompressor::GetVInt32Length(totalLen);
    } else {
        mPostingFile->Read((void*)(&totalLen), sizeof(uint32_t)).GetOrThrow();
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

    file_system::ByteSliceReaderPtr docListReader =
        std::dynamic_pointer_cast<file_system::ByteSliceReader>(mDocListReader);
    file_system::ByteSliceReaderPtr posListReader =
        std::dynamic_pointer_cast<file_system::ByteSliceReader>(mPosListReader);
    mDecoder->Init(mTermMeta, docListReader, posListReader, mTfBitmap, totalLen);
}

template <typename DictKey>
inline void OnDiskPackIndexIteratorTyped<DictKey>::DecodeTermMeta()
{
    TermMetaLoader tmLoader(_postingFormatOption);
    tmLoader.Load(mPostingFile, *mTermMeta);
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::DecodeDocList()
{
    uint32_t docSkipListLen = mPostingFile->ReadVUInt32().GetOrThrow();
    uint32_t docListLen = mPostingFile->ReadVUInt32().GetOrThrow();

    IE_LOG(TRACE1, "doc skip list length: [%u], doc list length: [%u]", docSkipListLen, docListLen);

    DELETE_BYTE_SLICE(mDocListSlice);
    mDocListSlice = util::ByteSlice::CreateObject(docListLen);

    int64_t cursor = mPostingFile->Tell() + docSkipListLen;
    ssize_t readLen = mPostingFile->Read(mDocListSlice->data, mDocListSlice->size, cursor).GetOrThrow();
    assert(readLen == (ssize_t)docListLen);
    (void)readLen;
    mDocListReader->Open(mDocListSlice);
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::DecodeTfBitmap()
{
    uint32_t bitmapBlockCount = mPostingFile->ReadVUInt32().GetOrThrow();
    uint32_t totalPosCount = mPostingFile->ReadVUInt32().GetOrThrow();

    int64_t cursor = mPostingFile->Tell();
    cursor += bitmapBlockCount * sizeof(uint32_t);
    uint32_t bitmapSizeInByte = util::Bitmap::GetDumpSize(totalPosCount);
    uint8_t* bitmapData = new uint8_t[bitmapSizeInByte];
    mPostingFile->Read((void*)bitmapData, bitmapSizeInByte, cursor).GetOrThrow();

    mTfBitmap.reset(new util::Bitmap());
    mTfBitmap->MountWithoutRefreshSetCount(totalPosCount, (uint32_t*)bitmapData, false);
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::DecodePosList()
{
    uint32_t posSkipListLen = mPostingFile->ReadVUInt32().GetOrThrow();
    uint32_t posListLen = mPostingFile->ReadVUInt32().GetOrThrow();

    DELETE_BYTE_SLICE(mPosListSlice);
    mPosListSlice = util::ByteSlice::CreateObject(posListLen);

    int64_t cursor = mPostingFile->Tell() + posSkipListLen;
    mPostingFile->Read(mPosListSlice->data, mPosListSlice->size, cursor).GetOrThrow();
    mPosListReader->Open(mPosListSlice);
}

#undef DELETE_BYTE_SLICE
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_ON_DISK_PACK_INDEX_ITERATOR_H
