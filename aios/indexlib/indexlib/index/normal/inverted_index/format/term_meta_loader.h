#ifndef __INDEXLIB_TERM_META_LOADER_H
#define __INDEXLIB_TERM_META_LOADER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/file_system/file_reader.h"

IE_NAMESPACE_BEGIN(index);

class TermMetaLoader
{
public:
    TermMetaLoader(const PostingFormatOption &option);
    TermMetaLoader() {}
    ~TermMetaLoader() {}
public:
    void Load(common::ByteSliceReader* sliceReader,
              index::TermMeta& termMeta) const;
    void Load(const file_system::FileReaderPtr& reader,
              index::TermMeta& termMeta) const;
    void Load(uint8_t*& dataCursor, size_t& leftSize,
              index::TermMeta& termMeta) const;
private:
    PostingFormatOption mOption;
};

DEFINE_SHARED_PTR(TermMetaLoader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TERM_META_LOADER_H
