#ifndef __INDEXLIB_SKIP_LIST_READER_H
#define __INDEXLIB_SKIP_LIST_READER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/common/byte_slice_reader.h"

IE_NAMESPACE_BEGIN(index);

class SkipListReader 
{
public:
    SkipListReader();
    SkipListReader(const SkipListReader& other);
    virtual ~SkipListReader();
public:
    virtual void Load(const util::ByteSliceList* byteSliceList,
                      uint32_t start, uint32_t end);

    virtual bool SkipTo(uint32_t queryKey, uint32_t& key, uint32_t& prevKey,
                        uint32_t& prevValue, uint32_t& valueDelta)
    { assert(false); return false; }

    uint32_t GetStart() const { return mStart; }
    uint32_t GetEnd() const { return mEnd; }

    const util::ByteSliceList* GetByteSliceList() const 
    { return mByteSliceReader.GetByteSliceList(); }

    int32_t GetSkippedItemCount() const {return mSkippedItemCount;}

    virtual uint32_t GetPrevTTF() const { return 0; }
    virtual uint32_t GetCurrentTTF() const { return 0; }

    virtual uint32_t GetLastValueInBuffer() const { return 0; }
    virtual uint32_t GetLastKeyInBuffer() const { return 0; }

protected:
    uint32_t mStart;
    uint32_t mEnd;
    common::ByteSliceReader mByteSliceReader;
    int32_t mSkippedItemCount;

private:
    friend class SkipListReaderTest;
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<SkipListReader> SkipListReaderPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SKIP_LIST_READER_H
