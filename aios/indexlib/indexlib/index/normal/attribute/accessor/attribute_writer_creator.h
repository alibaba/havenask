#ifndef __INDEXLIB_ATTRIBUTE_WRITER_CREATOR_H
#define __INDEXLIB_ATTRIBUTE_WRITER_CREATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <autil/mem_pool/Pool.h>
#include <tr1/memory>
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"

IE_NAMESPACE_BEGIN(index);

class AttributeWriterCreator
{
public:
    AttributeWriterCreator(){}
    virtual ~AttributeWriterCreator() {}

public:
    virtual FieldType GetAttributeType() const = 0;
    virtual AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const = 0;
};

typedef std::tr1::shared_ptr<AttributeWriterCreator> AttributeWriterCreatorPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_WRITER_CREATOR_H
