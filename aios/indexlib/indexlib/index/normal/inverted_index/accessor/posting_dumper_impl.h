#ifndef __INDEXLIB_POSTING_DUMPER_IMPL_H
#define __INDEXLIB_POSTING_DUMPER_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_dumper.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"

IE_NAMESPACE_BEGIN(index);

class PostingDumperImpl : public PostingDumper
{
public:
    PostingDumperImpl();
    ~PostingDumperImpl();

    PostingDumperImpl(PostingWriterResource* postingWriterResource, 
                      bool disableDictInline = false)
    {
        assert(postingWriterResource);
        mPostingFormatOption = postingWriterResource->postingFormatOption;
        mPostingWriter.reset(new PostingWriterImpl(postingWriterResource, disableDictInline));
    }

public:
    bool IsCompressedPostingHeader() const override { return true; }
    void Dump(const file_system::FileWriterPtr& postingFile) override;
    uint64_t GetDumpLength() const override;

private:
    index::PostingFormatOption mPostingFormatOption;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingDumperImpl);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_DUMPER_IMPL_H
