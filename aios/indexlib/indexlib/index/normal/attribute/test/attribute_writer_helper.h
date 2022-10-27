#ifndef __INDEXLIB_ATTRIBUTE_WRITER_HELPER_H
#define __INDEXLIB_ATTRIBUTE_WRITER_HELPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index);

class AttributeWriterHelper
{
public:
    AttributeWriterHelper();
    ~AttributeWriterHelper();

public:
    static void DumpWriter(AttributeWriter& writer,
                           const std::string& dirPath);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeWriterHelper);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_WRITER_HELPER_H
