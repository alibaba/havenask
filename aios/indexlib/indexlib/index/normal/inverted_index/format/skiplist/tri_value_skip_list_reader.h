#ifndef __INDEXLIB_TRI_VALUE_SKIP_LIST_READER_H
#define __INDEXLIB_TRI_VALUE_SKIP_LIST_READER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index/normal/inverted_index/format/skiplist/skip_list_reader.h"

IE_NAMESPACE_BEGIN(index);
class TriValueSkipListReader : public SkipListReader
{
    using SkipListReader::Load;
public:
    static const uint32_t ITEM_SIZE = sizeof(uint32_t) * 3;
    typedef uint32_t keytype_t;

    TriValueSkipListReader(bool isReferenceCompress = false);
    TriValueSkipListReader(const TriValueSkipListReader& other);
    ~TriValueSkipListReader();

public:
    virtual void Load(const util::ByteSliceList* byteSliceList, 
                      uint32_t start, uint32_t end, const uint32_t &itemCount);
    virtual bool SkipTo(uint32_t queryDocId, uint32_t& docId, uint32_t& prevDocId,
                        uint32_t& offset, uint32_t& delta);

    inline bool SkipTo(uint32_t queryDocId, uint32_t& docId,
                uint32_t& offset, uint32_t& delta);

    uint32_t GetPrevDocId() const
    { return mPrevDocId; }

    uint32_t GetCurrentDocId() const
    { return mCurrentDocId; }

    virtual uint32_t GetCurrentTTF() const
    { return mCurrentTTF; }

    virtual uint32_t GetPrevTTF() const
    { return mPrevTTF; }

protected:
    virtual bool LoadBuffer();

protected:
    uint32_t mCurrentDocId;
    uint32_t mCurrentOffset;
    uint32_t mCurrentTTF;
    uint32_t mPrevDocId;
    uint32_t mPrevOffset;
    uint32_t mPrevTTF;

protected:
    uint32_t mDocIdBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t mOffsetBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t mTTFBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t mCurrentCursor;
    uint32_t mNumInBuffer;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<TriValueSkipListReader> TriValueSkipListReaderPtr;

//////////////////////////////////////////////
//
inline bool TriValueSkipListReader::SkipTo(uint32_t queryDocId,
        uint32_t& docId, uint32_t& offset, uint32_t& delta)
{
    return SkipTo(queryDocId, docId, mPrevDocId, offset, delta);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SKIP_LIST_READER_H
