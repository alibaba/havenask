#ifndef __INDEXLIB_BITMAP_POSTING_DUMPER_H
#define __INDEXLIB_BITMAP_POSTING_DUMPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_dumper.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"

IE_NAMESPACE_BEGIN(index);

class BitmapPostingDumper : public PostingDumper
{
public:
    // BitmapPostingDumper();
    ~BitmapPostingDumper();

    BitmapPostingDumper(autil::mem_pool::PoolBase* pool = NULL)
    {
        mPostingWriter.reset(new BitmapPostingWriter(pool));
    }

    void Dump(const file_system::FileWriterPtr& postingFile) override;
    uint64_t GetDumpLength() const override;

public:
    bool IsCompressedPostingHeader() const override { return false; }
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BitmapPostingDumper);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_POSTING_DUMPER_H
