#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_MERGER_CREATOR_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_MERGER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_creator.h"
#include "indexlib/index/normal/attribute/accessor/uniq_encoded_var_num_attribute_merger.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class VarNumAttributeMergerCreator : public AttributeMergerCreator
{
public:
    FieldType GetAttributeType() const
    {
        return common::TypeInfo<T>::GetFieldType();
    }

    AttributeMerger* Create(bool isUniqEncoded, bool isUpdatable, bool needMergePatch) const
    {
        if (isUniqEncoded)
        {
            return new UniqEncodedVarNumAttributeMerger<T>(needMergePatch);
        }
        else
        {
            return new VarNumAttributeMerger<T>(needMergePatch);
        }
    }
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_UPDATABLE_VAR_NUM_ATTRIBUTE_MERGER_CREATOR_H
