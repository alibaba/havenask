#ifndef __INDEXLIB_BITMAP_INDEX_WRITER_H
#define __INDEXLIB_BITMAP_INDEX_WRITER_H

#include <tr1/memory>
#include <autil/mem_pool/pool_allocator.h>
#include "indexlib/util/hash_map.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/in_mem_bitmap_index_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class BitmapIndexWriter
{
public:
    typedef std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t> > HashKeyVector;
    typedef std::pair<uint64_t, BitmapPostingWriter*> PostingPair;
    typedef std::vector<PostingPair, autil::mem_pool::pool_allocator<PostingPair> > BitmapPostingVector;
    typedef util::HashMap<uint64_t, BitmapPostingWriter*> BitmapPostingTable;

public:
    BitmapIndexWriter(autil::mem_pool::Pool* byteSlicePool,
                      util::SimplePool* simplePool,
                      bool isNumberIndex,
                      bool isRealTime = false);
    ~BitmapIndexWriter();

public:
    void AddToken(const uint64_t hashKey, pospayload_t posPayload);
    void EndDocument(const document::IndexDocument& indexDocument);
    void Dump(const file_system::DirectoryPtr& dir,
              autil::mem_pool::PoolBase* dumpPool);

    InMemBitmapIndexSegmentReaderPtr CreateInMemReader();
    uint32_t GetDistinctTermCount() const { return mBitmapPostingTable.Size(); } 

public:
    //for test
    BitmapPostingWriter* GetBitmapPostingWriter(uint64_t key);
private:
    void DumpPosting(BitmapPostingWriter* writer, 
                     uint64_t key, 
                     const file_system::FileWriterPtr& postingFile,
                     TieredDictionaryWriter<dictkey_t>& dictionaryWriter);

    size_t GetDumpPostingFileLength() const;

private:
    autil::mem_pool::Pool* mByteSlicePool;
    util::SimplePool* mSimplePool;
    BitmapPostingTable mBitmapPostingTable;
    
    HashKeyVector mHashKeyVector;
    BitmapPostingVector mModifiedPosting;

    bool mIsRealTime;
    bool mIsNumberIndex;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BitmapIndexWriter);

inline BitmapPostingWriter* BitmapIndexWriter::GetBitmapPostingWriter(uint64_t key)
{
    BitmapPostingWriter** writer;
    writer = mBitmapPostingTable.Find(IndexWriter::GetRetrievalHashKey(
                    mIsNumberIndex, key));
    if (writer != NULL)
    {
        return *writer;
    }
    return NULL;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_INDEX_WRITER_H
