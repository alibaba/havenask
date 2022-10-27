#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/on_disk_bitmap_index_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_decoder.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/common_disk_tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_loader.h"
#include "indexlib/util/profiling.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);


IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, OnDiskBitmapIndexIterator);

OnDiskBitmapIndexIterator::OnDiskBitmapIndexIterator(
        const file_system::DirectoryPtr& indexDirectory)
    : OnDiskIndexIterator(indexDirectory, OPTION_FLAG_ALL)
    , mTermMeta(NULL)    
    , mBufferSize(DEFAULT_DATA_BUFFER_SIZE)
    , mDataBuffer(mDefaultDataBuffer)
    , mBufferLength(0)
{
}

OnDiskBitmapIndexIterator::~OnDiskBitmapIndexIterator() 
{
    FreeDataBuffer();
    DELETE_AND_SET_NULL(mTermMeta);
}

void OnDiskBitmapIndexIterator::Init()
{
    mDictionaryReader.reset(new CommonDiskTieredDictionaryReader());
    mDictionaryReader->Open(mIndexDirectory, BITMAP_DICTIONARY_FILE_NAME);
    mDictionaryIterator = mDictionaryReader->CreateIterator();

    //in the same file
    file_system::FileReaderPtr postingFileReader = 
        mIndexDirectory->CreateFileReader(
                BITMAP_POSTING_FILE_NAME, file_system::FSOT_BUFFERED);

    mDocListFile = DYNAMIC_POINTER_CAST(
            file_system::BufferedFileReader, postingFileReader);
    assert(mDocListFile);

    mDecoder.reset(new BitmapPostingDecoder());
    mTermMeta = new TermMeta();
}
 
bool OnDiskBitmapIndexIterator::HasNext() const
{
    return mDictionaryIterator->HasNext();
}

PostingDecoder* OnDiskBitmapIndexIterator::Next(dictkey_t& key)
{
    dictvalue_t value;
    mDictionaryIterator->Next(key, value);
    int64_t docListFileCursor = (int64_t)value;

    size_t readed = mDocListFile->Read((void*)(&mBufferLength), 
            sizeof(uint32_t), docListFileCursor);
    if (readed < sizeof(uint32_t))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Read file[%s] FAILED!",
                             mDocListFile->GetPath().c_str());
    }

    docListFileCursor += sizeof(uint32_t);
    if (mBufferLength > mBufferSize)
    {
        FreeDataBuffer();
        mDataBuffer = new uint8_t[mBufferLength];
        PROCESS_PROFILE_COUNT(memory, OnDiskBitmapIndexIterator, mBufferLength);
                              
        mBufferSize = mBufferLength;
    }

    readed = mDocListFile->Read(mDataBuffer, mBufferLength, docListFileCursor);
    if (readed < mBufferLength)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Read file[%s] FAILED!",
                             mDocListFile->GetPath().c_str());
    }

    uint8_t* dataCursor = mDataBuffer;
    size_t leftSize = mBufferLength;
    TermMetaLoader tmLoader;
    tmLoader.Load(dataCursor, leftSize, *mTermMeta);

    uint32_t bmSize = *(uint32_t*)dataCursor;
    dataCursor += sizeof(uint32_t);

    mDecoder->Init(mTermMeta, dataCursor, bmSize);
    return mDecoder.get();
}

void OnDiskBitmapIndexIterator::FreeDataBuffer()
{
    if (mDataBuffer != mDefaultDataBuffer)
    {
        PROCESS_PROFILE_COUNT(memory, OnDiskBitmapIndexIterator,
                              -sizeof(mDataBuffer));

        delete [] mDataBuffer;
        mDataBuffer = NULL;
    }
}

void OnDiskBitmapIndexIterator::Next(
        OnDiskBitmapIndexIterator::OnDiskBitmapIndexMeta& bitmapMeta)
{
    Next(bitmapMeta.key);
    getBufferInfo(bitmapMeta.size, bitmapMeta.data);
}

void OnDiskBitmapIndexIterator::getBufferInfo(uint32_t& bufferSize,
                   uint8_t*& dataBuffer)
{
    bufferSize = mBufferLength;
    dataBuffer = mDataBuffer;
}

IE_NAMESPACE_END(index);

