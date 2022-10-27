#ifndef __INDEXLIB_ON_DISK_BITMAP_INDEX_ITERATOR_H
#define __INDEXLIB_ON_DISK_BITMAP_INDEX_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_decoder.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/common_disk_tiered_dictionary_reader.h"

IE_NAMESPACE_BEGIN(index);

class OnDiskBitmapIndexIterator : public OnDiskIndexIterator
{
public:
    struct OnDiskBitmapIndexMeta
    {
        OnDiskBitmapIndexMeta()
            : key(0)
            , size(0)
            , data(NULL)
        {}

        dictkey_t key;
        uint32_t  size;
        uint8_t*  data;
    };
public:
    OnDiskBitmapIndexIterator(const file_system::DirectoryPtr& indexDirectory);
    ~OnDiskBitmapIndexIterator();

    // class Creator : public OnDiskIndexIteratorCreator
    // {
    // public:
    //     Creator() {}
    //     OnDiskIndexIterator* Create(
    //             const file_system::DirectoryPtr& indexDirectory) const override
    //     {
    //         return new OnDiskBitmapIndexIterator(indexDirectory);
    //     }
    // };

public:
    void Init() override;
    bool HasNext() const override;
    PostingDecoder* Next(dictkey_t& key) override;
    void Next(OnDiskBitmapIndexMeta& bitmapMeta);

private:
    void FreeDataBuffer();
    void getBufferInfo(uint32_t& bufferSize,
                       uint8_t*& dataBuffer);
private:
    CommonDiskTieredDictionaryReaderPtr mDictionaryReader;
    DictionaryIteratorPtr mDictionaryIterator;
    file_system::BufferedFileReaderPtr mDocListFile;
    index::TermMeta *mTermMeta;
    BitmapPostingDecoderPtr mDecoder;

    static const uint32_t FILE_BUFFER_SIZE = 1024 * 256;
    static const uint32_t DEFAULT_DATA_BUFFER_SIZE = 1024 * 256;
    uint32_t mBufferSize;
    uint8_t mDefaultDataBuffer[DEFAULT_DATA_BUFFER_SIZE];
    uint8_t *mDataBuffer;
    uint32_t mBufferLength;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskBitmapIndexIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ON_DISK_BITMAP_INDEX_ITERATOR_H
