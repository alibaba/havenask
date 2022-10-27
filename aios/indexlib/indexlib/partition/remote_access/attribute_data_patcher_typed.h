#ifndef __INDEXLIB_ATTRIBUTE_DATA_PATCHER_TYPED_H
#define __INDEXLIB_ATTRIBUTE_DATA_PATCHER_TYPED_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/attribute_value_type_traits.h"
#include "indexlib/partition/remote_access/attribute_data_patcher.h"
#include "indexlib/partition/remote_access/single_value_patch_data_writer.h"
#include "indexlib/partition/remote_access/var_num_patch_data_writer.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"

IE_NAMESPACE_BEGIN(partition);

template <typename T>
class AttributeDataPatcherTyped : public AttributeDataPatcher
{
public:
    AttributeDataPatcherTyped() {}
    ~AttributeDataPatcherTyped() {}

public:
    void AppendEncodedValue(const autil::ConstString& value) override
    {
        if (mPatchedDocCount >= mTotalDocCount)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "patchDocCount [%u] over totalDocCount [%u]",
                                 mPatchedDocCount, mTotalDocCount);
        }
        assert(mPatchDataWriter);
        mPatchDataWriter->AppendValue(value);
        mPatchedDocCount++;
    }

    void Close() override
    {
        if (!mPatchDataWriter)
        {
            return;
        }

        if (mPatchedDocCount == 0)
        {
            AppendAllDocByDefaultValue();
        }
        if (mPatchedDocCount > 0 && mPatchedDocCount < mTotalDocCount)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "patchDocCount [%u] not equal to totalDocCount [%u]",
                    mPatchedDocCount, mTotalDocCount);
        }
        mPatchDataWriter->Close();
        mPatchDataWriter.reset();
    }

    void AppendValue(T value)
    {
        std::string valueStr = index::AttributeValueTypeToString<T>::ToString(value);
        AppendFieldValue(valueStr);
    }

protected:
    bool DoInit(const config::MergeIOConfig& mergeIOConfig) override
    {
        if (mAttrConf->IsMultiValue() || mAttrConf->GetFieldType() == ft_string)
        {
            mPatchDataWriter.reset(new VarNumPatchDataWriter());
        }
        else
        {
            FieldType ft = mAttrConf->GetFieldType();
            switch (ft)
            {
#define MACRO(field_type)                                               \
                case field_type:                                        \
                {                                                       \
                    typedef typename config::FieldTypeTraits<field_type>::AttrItemType TT; \
                    mPatchDataWriter.reset(new SingleValuePatchDataWriter<TT>()); \
                    break;                                              \
                }
                NUMBER_FIELD_MACRO_HELPER(MACRO);
#undef MACRO
            default:
                assert(false);
                IE_LOG(ERROR, "unsupport field type [%s]",
                       config::FieldConfig::FieldTypeToStr(ft));
                return false;
            }
        }
        return mPatchDataWriter->Init(mAttrConf, mergeIOConfig, mDirPath);
    }
};

//DEFINE_SHARED_PTR(AttributeDataPatcherTyped);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ATTRIBUTE_DATA_PATCHER_TYPED_H
