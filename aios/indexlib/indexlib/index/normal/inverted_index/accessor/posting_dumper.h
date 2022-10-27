#ifndef __INDEXLIB_POSTING_DUMPER_H
#define __INDEXLIB_POSTING_DUMPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer.h"

IE_NAMESPACE_BEGIN(index);

class PostingDumper
{
public:
    PostingDumper()
        : mTermPayload(0)
    {
    }
    ~PostingDumper() {}

public:
    void SetTermPayload(termpayload_t termPayload) { mTermPayload = termPayload; }
    virtual uint64_t GetDumpLength() const = 0 ;
    virtual bool IsCompressedPostingHeader() const = 0;

    virtual PostingWriterPtr GetPostingWriter() { return mPostingWriter; }
    virtual bool GetDictInlinePostingValue(uint64_t& inlinePostingValue) 
    {
        assert(mPostingWriter);
        return mPostingWriter->GetDictInlinePostingValue(inlinePostingValue);
    }
    virtual void Dump(const file_system::FileWriterPtr& postingFile) = 0;
    virtual df_t GetDocFreq() const { return mPostingWriter->GetDF(); }
    virtual ttf_t GetTotalTF() const { return mPostingWriter->GetTotalTF(); }
    termpayload_t GetTermPayload() const
    {
        return mTermPayload;
    }


protected:
    PostingWriterPtr mPostingWriter;
    termpayload_t mTermPayload;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingDumper);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_DUMPER_H
