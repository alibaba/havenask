#ifndef __INDEXLIB_STRING_ATTRIBUTE_MERGER_H
#define __INDEXLIB_STRING_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/uniq_encoded_var_num_attribute_merger.h"

IE_NAMESPACE_BEGIN(index);

class StringAttributeMergerCreator : public AttributeMergerCreator
{
public:
    FieldType GetAttributeType() const
    {
        return ft_string;
    }

    AttributeMerger* Create(bool isUniqEncoded, bool isUpdatable, bool needMergePatch) const
    {
        if (isUniqEncoded)
        {
            return new UniqEncodedVarNumAttributeMerger<char>(needMergePatch);
        }
        else
        {
            return new VarNumAttributeMerger<char>(needMergePatch);
        }
    }
};
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_STRING_ATTRIBUTE_MERGER_H
