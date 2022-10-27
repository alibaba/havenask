#ifndef __INDEXLIB_COMPRESS_OFFSET_READER_H
#define __INDEXLIB_COMPRESS_OFFSET_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/numeric_compress/equivalent_compress_reader.h"
#include "indexlib/file_system/slice_file_reader.h"

IE_NAMESPACE_BEGIN(index);

class CompressOffsetReader
{
public:
    CompressOffsetReader();
    ~CompressOffsetReader();

public:
    void Init(uint8_t* buffer, uint64_t bufferLen, uint32_t docCount,
              file_system::SliceFileReaderPtr expandSliceFile = 
              file_system::SliceFileReaderPtr());

    inline uint64_t GetOffset(docid_t docId) const __ALWAYS_INLINE;
    bool SetOffset(docid_t docId, uint64_t offset);

    inline bool IsU32Offset() const __ALWAYS_INLINE;

    common::EquivalentCompressUpdateMetrics GetUpdateMetrics()
    {
        if (mU64CompressReader)
        {
            return mU64CompressReader->GetUpdateMetrics();
        }
        assert(mU32CompressReader);
        return mU32CompressReader->GetUpdateMetrics();
    }

    uint32_t GetDocCount() const
    {
        if (mU64CompressReader)
        {
            return mU64CompressReader->Size() - 1;
        }
        assert(mU32CompressReader);
        return mU32CompressReader->Size() - 1;
    }

private:
    common::EquivalentCompressReader<uint32_t>* mU32CompressReader;
    common::EquivalentCompressReader<uint64_t>* mU64CompressReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressOffsetReader);
////////////////////////////////////////////////////////////////

inline uint64_t CompressOffsetReader::GetOffset(docid_t docId) const
{
    if (mU64CompressReader)
    {
        return (*mU64CompressReader)[docId];
    }
    assert(mU32CompressReader);
    return (*mU32CompressReader)[docId];
}

inline bool CompressOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    if (mU64CompressReader)
    {
        return mU64CompressReader->Update(docId, offset);
    }
    assert(mU32CompressReader);
    return mU32CompressReader->Update(docId, (uint32_t)offset);
}

inline bool CompressOffsetReader::IsU32Offset() const
{
    return mU32CompressReader != NULL;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_COMPRESS_OFFSET_READER_H
