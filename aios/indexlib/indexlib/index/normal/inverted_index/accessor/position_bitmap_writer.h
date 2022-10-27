#ifndef __INDEXLIB_POSITION_BITMAP_WRITER_H
#define __INDEXLIB_POSITION_BITMAP_WRITER_H

#include <tr1/memory>
#include <autil/mem_pool/pool_allocator.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/file_writer.h"
#include "indexlib/util/simple_pool.h"
#include "indexlib/util/expandable_bitmap.h"
#include "indexlib/file_system/file_writer.h"

IE_NAMESPACE_BEGIN(index);

class PositionBitmapWriter
{
public:
    typedef autil::mem_pool::pool_allocator<uint32_t> UI32Allocator;
    //TODO: delete
    virtual void Dump(storage::FileWriter* file, uint32_t bitCount);
    
public:
    PositionBitmapWriter(util::SimplePool* simplePool)
        : mBitmap(1, false, simplePool)
        , mBlockOffsets(UI32Allocator(simplePool))
    {}

    virtual ~PositionBitmapWriter() {}

public:
    // virtual for google_mock
    virtual void Set(uint32_t index);
    virtual void Resize(uint32_t size);
    virtual void EndDocument(uint32_t df, uint32_t totalPosCount);
    virtual uint32_t GetDumpLength(uint32_t bitCount) const;
    virtual void Dump(const file_system::FileWriterPtr& file, uint32_t bitCount);
    const util::ExpandableBitmap& GetBitmap() const
    {
        return mBitmap;
    }
private:
    // represent positions as bitmap.
    // bit 1 represent first occ in doc.
    // bit 0 represent other position in doc.
    util::ExpandableBitmap mBitmap;
    std::vector<uint32_t, UI32Allocator> mBlockOffsets;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PositionBitmapWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITION_BITMAP_WRITER_H
