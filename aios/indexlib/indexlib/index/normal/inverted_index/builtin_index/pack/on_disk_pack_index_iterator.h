#ifndef __INDEXLIB_ON_DISK_PACK_INDEX_ITERATOR_H
#define __INDEXLIB_ON_DISK_PACK_INDEX_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/util/bitmap.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/posting_decoder_impl.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_loader.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index);

#define DELETE_BYTE_SLICE(slice) do {                   \
        if (slice != NULL)                              \
        {                                               \
            util::ByteSlice::DestroyObject(slice);    \
            slice = NULL;                               \
        }                                               \
    } while(0)

template <typename DictKey>
class OnDiskPackIndexIteratorTyped : public OnDiskIndexIterator
{
private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 1024 * 1024 * 2; // 2M

public:
    OnDiskPackIndexIteratorTyped(
            const config::IndexConfigPtr& indexConfig,
            const file_system::DirectoryPtr& indexDirectory,
            const index::PostingFormatOption& postingFormatOption,
            const config::MergeIOConfig &ioConfig = config::MergeIOConfig());

    virtual ~OnDiskPackIndexIteratorTyped();

    DECLARE_ON_DISK_INDEX_ITERATOR_CREATOR(OnDiskPackIndexIteratorTyped<DictKey>);
    // class Creator : public OnDiskIndexIteratorCreator
    // {
    // public:
    //     Creator(const index::PostingFormatOption& postingFormatOption, 
    //             const config::MergeIOConfig &ioConfig,
    //             const config::IndexConfigPtr& indexConfig)                
    //         : mPostingFormatOption(postingFormatOption)
    //         , mIOConfig(ioConfig)
    //         , mIndexConfig(indexConfig)
    //     {
    //     }

    //     OnDiskIndexIterator* Create(
    //             const index_base::SegmentData& segData) const override
    //     {
    //         DirectoryPtr indexDirectory = segData.GetIndexDirectory(
    //                 mIndexConfig->GetIndexName(), false);
    //         if (indexDirectory &&
    //             indexDirectory->IsExist(DICTIONARY_FILE_NAME))
    //         {
    //             return new OnDiskPackIndexIteratorTyped<DictKey>(
    //                     indexDirectory, mPostingFormatOption, mIOConfig);                
    //         }
    //         return NULL;
    //     }

    // private:
    //     index::PostingFormatOption mPostingFormatOption;
    //     const config::MergeIOConfig &mIOConfig;
    // };

public:
    void Init() override;
    bool HasNext() const override;
    PostingDecoder* Next(dictkey_t& key) override;

private:
    virtual void CreatePostingDecoder()
    {
        mDecoder.reset(new index::PostingDecoderImpl(
                        this->mPostingFormatOption));
    }

private:
    void DecodeTermMeta();
    void DecodeDocList();
    void DecodeTfBitmap();    
    void DecodePosList();

    void InitDecoderForDictInlinePosting(dictvalue_t value);
    void InitDecoderForNormalPosting(dictvalue_t value);

protected:
    config::IndexConfigPtr mIndexConfig;
    DictionaryIteratorPtr mDictionaryIterator;
    file_system::BufferedFileReaderPtr mPostingFile;

    util::ByteSlice *mDocListSlice;
    util::ByteSlice *mPosListSlice;

    common::ByteSliceReaderPtr mDocListReader;
    common::ByteSliceReaderPtr mPosListReader;
    util::BitmapPtr mTfBitmap;
    
    index::TermMeta *mTermMeta;
    index::PostingDecoderImplPtr mDecoder;
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
OnDiskPackIndexIteratorTyped<DictKey>::OnDiskPackIndexIteratorTyped(
        const config::IndexConfigPtr& indexConfig,        
        const file_system::DirectoryPtr& indexDirectory,
        const index::PostingFormatOption& postingFormatOption,
        const config::MergeIOConfig &ioConfig)
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
    if (mIndexDirectory->IsExist(INDEX_FORMAT_OPTION_FILE_NAME))
    {
        std::string indexFormatStr;
        mIndexDirectory->Load(INDEX_FORMAT_OPTION_FILE_NAME, indexFormatStr);
        index::IndexFormatOption option =
            index::IndexFormatOption::FromString(indexFormatStr);
        mPostingFormatOption = option.GetPostingFormatOption();
    }
    else
    {
        mPostingFormatOption.SetIsCompressedPostingHeader(false);
    }

    
    DictionaryReaderPtr dictionaryReader(DictionaryCreator::CreateDiskReader(mIndexConfig));
    dictionaryReader->Open(mIndexDirectory, DICTIONARY_FILE_NAME);
    mDictionaryIterator = dictionaryReader->CreateIterator();

    file_system::FileReaderPtr postingFileReader = 
        mIndexDirectory->CreateFileReader(
                POSTING_FILE_NAME, file_system::FSOT_BUFFERED);

    mPostingFile = DYNAMIC_POINTER_CAST(
            file_system::BufferedFileReader, postingFileReader);
    assert(mPostingFile);

    mPostingFile->ResetBufferParam(this->mIOConfig.readBufferSize, 
                                   this->mIOConfig.enableAsyncRead);

    mDocListReader.reset(new common::ByteSliceReader());
    if (this->mPostingFormatOption.HasTfBitmap())
    {
        mTfBitmap.reset(new util::Bitmap());
    }
    if (this->mPostingFormatOption.HasPositionList())
    {
        mPosListReader.reset(new common::ByteSliceReader());
    }

    mTermMeta = new index::TermMeta();
    CreatePostingDecoder();
}

template <typename DictKey>
bool OnDiskPackIndexIteratorTyped<DictKey>::HasNext() const
{
    return mDictionaryIterator->HasNext();
}

template <typename DictKey>
PostingDecoder* OnDiskPackIndexIteratorTyped<DictKey>::Next(dictkey_t& key)
{
    dictvalue_t value;
    mDictionaryIterator->Next(key, value);
    mDecoder->SetCurrentTermHashKey(key);
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(value))
    {
        InitDecoderForDictInlinePosting(value);
    }
    else
    {
        InitDecoderForNormalPosting(value);
    }
    
    if (!HasNext())
    {
        //not close posting file in destruct
        mPostingFile->Close();
    }
    return mDecoder.get();
}

template <typename DictKey>
inline void OnDiskPackIndexIteratorTyped<DictKey>::InitDecoderForDictInlinePosting(dictvalue_t value)
{
    mDecoder->Init(mTermMeta, value);
}

template <typename DictKey>
inline void OnDiskPackIndexIteratorTyped<DictKey>::InitDecoderForNormalPosting(dictvalue_t value)
{
    int64_t fileCursor = 0;
    ShortListOptimizeUtil::GetOffset(value, fileCursor);

    uint32_t totalLen = 0;
    mPostingFile->Seek(fileCursor);
    if (mPostingFormatOption.IsCompressedPostingHeader())
    {
        totalLen = mPostingFile->ReadVUInt32();
    }
    else
    {
        mPostingFile->Read((void*)(&totalLen), sizeof(uint32_t));
    }
    
    DecodeTermMeta();
    DecodeDocList();

    if (this->mPostingFormatOption.HasTfBitmap())
    {
        DecodeTfBitmap();
    }

    if (this->mPostingFormatOption.HasPositionList())
    {
        DecodePosList();
    }
    
    common::ByteSliceReaderPtr docListReader = 
        std::tr1::dynamic_pointer_cast<common::ByteSliceReader>(mDocListReader);
    common::ByteSliceReaderPtr posListReader = 
        std::tr1::dynamic_pointer_cast<common::ByteSliceReader>(mPosListReader);

    mDecoder->Init(mTermMeta, docListReader, posListReader, mTfBitmap);
}

template <typename DictKey>
inline void OnDiskPackIndexIteratorTyped<DictKey>::DecodeTermMeta()
{
    index::TermMetaLoader tmLoader(mPostingFormatOption);
    tmLoader.Load(mPostingFile, *mTermMeta);
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::DecodeDocList()
{
    uint32_t docSkipListLen = mPostingFile->ReadVUInt32();
    uint32_t docListLen = mPostingFile->ReadVUInt32();
    
    IE_LOG(TRACE1, "doc skip list length: [%u], doc list length: [%u]",
           docSkipListLen, docListLen);

    DELETE_BYTE_SLICE(mDocListSlice);
    mDocListSlice = util::ByteSlice::CreateObject(docListLen);
    
    int64_t cursor = mPostingFile->Tell() + docSkipListLen;
    ssize_t readLen = mPostingFile->Read(
            mDocListSlice->data, mDocListSlice->size, cursor);
    assert(readLen == (ssize_t)docListLen);
    (void)readLen;
    mDocListReader->Open(mDocListSlice);
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::DecodeTfBitmap()
{
    uint32_t bitmapBlockCount = mPostingFile->ReadVUInt32();
    uint32_t totalPosCount = mPostingFile->ReadVUInt32();
    
    int64_t cursor = mPostingFile->Tell();
    cursor += bitmapBlockCount * sizeof(uint32_t);
    uint32_t bitmapSizeInByte = util::Bitmap::GetDumpSize(totalPosCount);
    uint8_t *bitmapData = new uint8_t[bitmapSizeInByte];
    mPostingFile->Read((void *)bitmapData, bitmapSizeInByte, cursor);
    
    mTfBitmap.reset(new util::Bitmap());
    mTfBitmap->MountWithoutRefreshSetCount(totalPosCount,
            (uint32_t*)bitmapData, false);
}

template <typename DictKey>
void OnDiskPackIndexIteratorTyped<DictKey>::DecodePosList()
{
    uint32_t posSkipListLen = mPostingFile->ReadVUInt32();
    uint32_t posListLen = mPostingFile->ReadVUInt32();

    DELETE_BYTE_SLICE(mPosListSlice);
    mPosListSlice = util::ByteSlice::CreateObject(posListLen);
    
    int64_t cursor = mPostingFile->Tell() + posSkipListLen;
    mPostingFile->Read(mPosListSlice->data, mPosListSlice->size, cursor);
    mPosListReader->Open(mPosListSlice);
}    

#undef DELETE_BYTE_SLICE

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ON_DISK_PACK_INDEX_ITERATOR_H
