#ifndef __INDEXLIB_IN_MEM_TRI_VALUE_SKIP_LIST_READER_H
#define __INDEXLIB_IN_MEM_TRI_VALUE_SKIP_LIST_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/tri_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice_reader.h"

IE_NAMESPACE_BEGIN(index);

class InMemTriValueSkipListReader : public TriValueSkipListReader
{
public:
    InMemTriValueSkipListReader(autil::mem_pool::Pool *sessionPool = NULL);
    ~InMemTriValueSkipListReader();

public:
    virtual void Load(const util::ByteSliceList* byteSliceList, 
                      uint32_t start, uint32_t end, const uint32_t &itemCount)
    { assert(false); }

    void Load(index::BufferedByteSlice* postingBuffer);

    virtual uint32_t GetLastValueInBuffer() const;
    virtual uint32_t GetLastKeyInBuffer() const;

protected:
    virtual bool LoadBuffer();

private:
    autil::mem_pool::Pool *mSessionPool;
    index::BufferedByteSlice* mSkipListBuffer;
    index::BufferedByteSliceReader mSkipListReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemTriValueSkipListReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_TRI_VALUE_SKIP_LIST_READER_H
