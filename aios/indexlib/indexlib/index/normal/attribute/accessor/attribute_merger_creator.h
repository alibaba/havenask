#ifndef __INDEXLIB_ATTRIBUTE_MERGER_CREATOR_H
#define __INDEXLIB_ATTRIBUTE_MERGER_CREATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"

IE_NAMESPACE_BEGIN(index);

#define DECLARE_ATTRIBUTE_MERGER_CREATOR(classname, indextype)          \
    class Creator : public AttributeMergerCreator                       \
    {                                                                   \
    public:                                                             \
        FieldType GetAttributeType() const {return indextype;}          \
        AttributeMerger* Create() const {return (new classname());} \
    };

class AttributeMergerCreator
{
public:
    AttributeMergerCreator(){}
    virtual ~AttributeMergerCreator() {}

public:
    virtual FieldType GetAttributeType() const = 0;
    virtual AttributeMerger* Create(bool isUniqEncoded,
                                    bool isUpdatable,
                                    bool needMergePatch) const = 0;
};

DEFINE_SHARED_PTR(AttributeMergerCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_MERGER_CREATOR_H
