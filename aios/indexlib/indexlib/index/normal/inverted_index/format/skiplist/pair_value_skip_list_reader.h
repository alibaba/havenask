#ifndef __INDEXLIB_PAIR_VALUE_SKIP_LIST_READER_H
#define __INDEXLIB_PAIR_VALUE_SKIP_LIST_READER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index/normal/inverted_index/format/skiplist/skip_list_reader.h"

IE_NAMESPACE_BEGIN(index);
class PairValueSkipListReader : public SkipListReader
{
private:
    using SkipListReader::Load;
public:
    static const uint32_t ITEM_SIZE = sizeof(uint32_t) * 2;
    typedef uint32_t keytype_t;
    PairValueSkipListReader(bool isReferenceCompress = false);
    PairValueSkipListReader(const PairValueSkipListReader& other);
    virtual ~PairValueSkipListReader();

public:
    virtual void Load(const util::ByteSliceList* byteSliceList,
                      uint32_t start, uint32_t end, const uint32_t& itemCount);
    virtual bool SkipTo(uint32_t queryKey, uint32_t& key, uint32_t& prevKey,
                        uint32_t& value, uint32_t& delta);
    inline bool SkipTo(uint32_t queryKey, uint32_t& key,
                       uint32_t& value, uint32_t& delta);
    
    uint32_t GetPrevKey() const
    { return mPrevKey; }

    uint32_t GetCurrentKey() const
    { return mCurrentKey; }

    virtual bool LoadBuffer();

    virtual uint32_t GetLastValueInBuffer() const;
    virtual uint32_t GetLastKeyInBuffer() const;

public:
    // for test
    void SetPrevKey(uint32_t prevKey) { mPrevKey = prevKey; }
    void SetCurrentKey(uint32_t curKey) { mCurrentKey = curKey; }

protected:
    uint32_t mCurrentKey;
    uint32_t mCurrentValue;
    uint32_t mPrevKey;
    uint32_t mPrevValue;

protected:
    uint32_t mKeyBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t mValueBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t mCurrentCursor;
    uint32_t mNumInBuffer;
    uint32_t* mKeyBufferBase;
    uint32_t* mValueBufferBase;
    bool mIsReferenceCompress;
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<PairValueSkipListReader> PairValueSkipListReaderPtr;

///////////////////////////////////////////////
inline bool PairValueSkipListReader::SkipTo(uint32_t queryKey, uint32_t& key,
        uint32_t& value, uint32_t& delta)
{
    return SkipTo(queryKey, key, mPrevKey, value, delta);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PAIR_VALUE_SKIP_LIST_READER_H
