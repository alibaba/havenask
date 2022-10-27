#ifndef __INDEXLIB_TERM_META_DUMPER_H
#define __INDEXLIB_TERM_META_DUMPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/file_system/file_writer.h"

IE_NAMESPACE_BEGIN(index);

class TermMetaDumper
{
public:
    TermMetaDumper(const PostingFormatOption &option);
    TermMetaDumper() {}
    ~TermMetaDumper() {}
public:
    uint32_t CalculateStoreSize(const index::TermMeta& termMeta) const;
    void Dump(const file_system::FileWriterPtr& file,
              const index::TermMeta& termMeta) const;
private:
    PostingFormatOption mOption;
};

DEFINE_SHARED_PTR(TermMetaDumper);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TERM_META_DUMPER_H
