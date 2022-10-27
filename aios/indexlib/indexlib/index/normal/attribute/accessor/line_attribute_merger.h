#ifndef __INDEXLIB_LINE_ATTRIBUTE_MERGER_H
#define __INDEXLIB_LINE_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger.h"

IE_NAMESPACE_BEGIN(index);

class LineAttributeMerger : VarNumAttributeMerger<double>
{
public:
    LineAttributeMerger(bool needMergePatch = false)
        : VarNumAttributeMerger<double>(needMergePatch)
    {}
    ~LineAttributeMerger(){}
public:
    class LineAttributeMergerCreator : public AttributeMergerCreator
    {
    public:
        FieldType GetAttributeType() const
        {
            return ft_line;
        }
        
        AttributeMerger* Create(bool isUniqEncoded, bool isUpdatable, bool needMergePatch) const
        {
            return new LineAttributeMerger(needMergePatch);
        }
    };

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LineAttributeMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LINE_ATTRIBUTE_MERGER_H
