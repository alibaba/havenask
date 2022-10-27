#ifndef __INDEXLIB_BUFFERED_SKIP_LIST_WRITER_H
#define __INDEXLIB_BUFFERED_SKIP_LIST_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"

IE_NAMESPACE_BEGIN(index);

class BufferedSkipListWriter : public index::BufferedByteSlice
{
public:
    BufferedSkipListWriter(autil::mem_pool::Pool* byteSlicePool,
                           autil::mem_pool::RecyclePool* bufferPool,
                           CompressMode compressMode = PFOR_DELTA_COMPRESS_MODE);
    virtual ~BufferedSkipListWriter();

public:
    void AddItem(uint32_t deltaValue1);
    void AddItem(uint32_t key, uint32_t value1);
    void AddItem(uint32_t key, uint32_t value1, uint32_t value2);

    size_t FinishFlush();

    void Dump(const file_system::FileWriterPtr& file);
    size_t EstimateDumpSize() const;

public:
    // include sizeof(BufferedSkipListWriter), ByteSliceWriter & ShortBuffe use
    // number 250 is estimate by unit test
    static constexpr size_t ESTIMATE_INIT_MEMORY_USE = 250;

private:
    static const uint32_t INVALID_LAST_KEY = (uint32_t)-1;
    bool IsReferenceCompress() const { return mLastKey == INVALID_LAST_KEY; }

protected:
    virtual size_t DoFlush(uint8_t compressMode);

private:
    uint32_t mLastKey;
    uint32_t mLastValue1;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BufferedSkipListWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUFFERED_SKIP_LIST_WRITER_H
