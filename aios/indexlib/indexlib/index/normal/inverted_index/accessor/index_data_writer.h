#ifndef __INDEXLIB_INDEX_DATA_WRITER_H
#define __INDEXLIB_INDEX_DATA_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/file_system/file_writer.h"

IE_NAMESPACE_BEGIN(index);

struct IndexDataWriter
{
public:
    bool IsValid() const { return dictWriter.get() != NULL; }
    void Reset() 
    {
        dictWriter.reset();
        postingWriter.reset();
    }
    DictionaryWriterPtr dictWriter;
    file_system::FileWriterPtr postingWriter;
};
DEFINE_SHARED_PTR(IndexDataWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_DATA_WRITER_H
