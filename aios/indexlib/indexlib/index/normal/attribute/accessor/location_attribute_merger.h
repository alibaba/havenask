#ifndef __INDEXLIB_LOCATION_ATTRIBUTE_MERGER_H
#define __INDEXLIB_LOCATION_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger.h"

IE_NAMESPACE_BEGIN(index);

class LocationAttributeMerger : VarNumAttributeMerger<double>
{
public:
    LocationAttributeMerger(bool needMergePatch = false)
        : VarNumAttributeMerger<double>(needMergePatch)
    {}
    ~LocationAttributeMerger(){}
public:
    class LocationAttributeMergerCreator : public AttributeMergerCreator
    {
    public:
        FieldType GetAttributeType() const
        {
            return ft_location;
        }
        
        AttributeMerger* Create(bool isUniqEncoded, bool isUpdatable, bool needMergePatch) const
        {
            return new LocationAttributeMerger(needMergePatch);
        }
    };

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocationAttributeMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LOCATION_ATTRIBUTE_MERGER_H
