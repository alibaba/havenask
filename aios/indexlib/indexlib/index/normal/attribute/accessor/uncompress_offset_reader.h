#ifndef __INDEXLIB_UNCOMPRESS_OFFSET_READER_H
#define __INDEXLIB_UNCOMPRESS_OFFSET_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/slice_file_reader.h"

IE_NAMESPACE_BEGIN(index);

class UncompressOffsetReader
{
public:
    UncompressOffsetReader();
    ~UncompressOffsetReader();

public:
    void Init(uint8_t* buffer, uint64_t bufferLen, uint32_t docCount,
              file_system::SliceFileReaderPtr u64SliceFileReader = 
              file_system::SliceFileReaderPtr());

    inline uint64_t GetOffset(docid_t docId) const __ALWAYS_INLINE;

    bool SetOffset(docid_t docId, uint64_t offset);

    inline bool IsU32Offset() const __ALWAYS_INLINE;

    void ExtendU32OffsetToU64Offset(
            const file_system::SliceFileReaderPtr& u64SliceFile);

    uint32_t GetDocCount() const
    { return mDocCount; }

private:
    static void ExtendU32OffsetToU64Offset(uint32_t* u32Offset,
            uint64_t* u64Offset, uint32_t count);

private:
    // u32->u64, ensure attribute reader used by modifier & reader is same one
    volatile uint64_t* mU64Buffer;
    uint32_t* mU32Buffer;
    uint32_t mDocCount;

private:
    friend class UncompressOffsetReaderTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UncompressOffsetReader);
///////////////////////////////////////////////////////////
inline uint64_t UncompressOffsetReader::GetOffset(docid_t docId) const
{
    if (IsU32Offset())
    {
        return mU32Buffer[docId];
    }
    return mU64Buffer[docId];
}

inline bool UncompressOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    if (!IsU32Offset())
    {
        mU64Buffer[docId] = offset;
        return true;
    }
    mU32Buffer[docId] = (uint32_t)offset;
    return true;
}

inline bool UncompressOffsetReader::IsU32Offset() const
{
    return mU64Buffer == NULL;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_UNCOMPRESS_OFFSET_READER_H
