#ifndef __INDEXLIB_POLYGON_ATTRIBUTE_MERGER_H
#define __INDEXLIB_POLYGON_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger.h"

IE_NAMESPACE_BEGIN(index);

class PolygonAttributeMerger : VarNumAttributeMerger<double>
{
public:
    PolygonAttributeMerger(bool needMergePatch = false)
        : VarNumAttributeMerger<double>(needMergePatch)
    {}
    ~PolygonAttributeMerger(){}
public:
    class PolygonAttributeMergerCreator : public AttributeMergerCreator
    {
    public:
        FieldType GetAttributeType() const
        {
            return ft_polygon;
        }
        
        AttributeMerger* Create(bool isUniqEncoded, bool isUpdatable, bool needMergePatch) const
        {
            return new PolygonAttributeMerger(needMergePatch);
        }
    };

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PolygonAttributeMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POLYGON_ATTRIBUTE_MERGER_H
