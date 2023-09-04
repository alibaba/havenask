#ifndef __INDEXLIB_ATTRIBUTE_WRITER_HELPER_H
#define __INDEXLIB_ATTRIBUTE_WRITER_HELPER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class AttributeWriterHelper
{
public:
    AttributeWriterHelper();
    ~AttributeWriterHelper();

public:
    static void DumpWriter(AttributeWriter& writer, const std::string& dirPath);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeWriterHelper);
}} // namespace indexlib::index

#endif //__INDEXLIB_ATTRIBUTE_WRITER_HELPER_H
