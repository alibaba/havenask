
#ifndef __INDEXLIB_ATTRIBUTE_READER_CREATOR_H
#define __INDEXLIB_ATTRIBUTE_READER_CREATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"

IE_NAMESPACE_BEGIN(index);

#define DECLARE_ATTRIBUTE_READER_CREATOR(classname, indextype)          \
    class Creator : public AttributeReaderCreator                       \
    {                                                                   \
    public:                                                             \
        FieldType GetAttributeType() const {return indextype;}          \
        AttributeReader* Create() const {return new classname();} \
    };

class AttributeReaderCreator
{
public:
    AttributeReaderCreator(){}
    virtual ~AttributeReaderCreator() {}

public:
    virtual FieldType GetAttributeType() const = 0;
    virtual AttributeReader* Create(AttributeMetrics* metrics = NULL) const = 0;
};

typedef std::tr1::shared_ptr<AttributeReaderCreator> AttributeReaderCreatorPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_READER_CREATOR_H
