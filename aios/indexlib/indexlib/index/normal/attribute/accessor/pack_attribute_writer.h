#ifndef __INDEXLIB_PACK_ATTRIBUTE_WRITER_H
#define __INDEXLIB_PACK_ATTRIBUTE_WRITER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_writer.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/util/mem_buffer.h"
#include "indexlib/util/simple_pool.h"

IE_NAMESPACE_BEGIN(index);

class PackAttributeWriter : public StringAttributeWriter
{
public:
    typedef common::PackAttributeFormatter::PackAttributeField PackAttributeField;
    typedef common::PackAttributeFormatter::PackAttributeFields PackAttributeFields;

public:
    PackAttributeWriter(
            const config::PackAttributeConfigPtr& packAttrConfig);            
    
    ~PackAttributeWriter();
    
public:
    bool UpdateEncodeFields(
        docid_t docId, const PackAttributeFields& packAttrFields);

private:
    static const size_t PACK_ATTR_BUFF_INIT_SIZE = 256 * 1024; // 256KB

private:
    config::PackAttributeConfigPtr mPackAttrConfig;
    common::PackAttributeFormatterPtr mPackAttrFormatter;
    util::MemBuffer mBuffer;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_ATTRIBUTE_WRITER_H
