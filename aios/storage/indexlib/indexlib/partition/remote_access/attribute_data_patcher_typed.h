/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_ATTRIBUTE_DATA_PATCHER_TYPED_H
#define __INDEXLIB_ATTRIBUTE_DATA_PATCHER_TYPED_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/common/AttributeValueTypeTraits.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/attribute_data_patcher.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/partition/remote_access/single_value_patch_data_writer.h"
#include "indexlib/partition/remote_access/var_num_patch_data_writer.h"

namespace indexlib { namespace partition {

template <typename T>
class AttributeDataPatcherTyped : public AttributeDataPatcher
{
public:
    AttributeDataPatcherTyped() {}
    ~AttributeDataPatcherTyped() {}

public:
    void AppendEncodedValue(const autil::StringView& value) override
    {
        if (mPatchedDocCount >= mTotalDocCount) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "patchDocCount [%u] over totalDocCount [%u]", mPatchedDocCount,
                                 mTotalDocCount);
        }
        assert(mPatchDataWriter);
        mPatchDataWriter->AppendValue(value);
        mPatchedDocCount++;
    }

    void Close() override
    {
        if (!mPatchDataWriter) {
            return;
        }

        if (mPatchedDocCount == 0) {
            AppendAllDocByDefaultValue();
        }
        if (mPatchedDocCount > 0 && mPatchedDocCount < mTotalDocCount) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "patchDocCount [%u] not equal to totalDocCount [%u]",
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
        if (mAttrConf->IsMultiValue() || mAttrConf->GetFieldType() == ft_string) {
            mPatchDataWriter.reset(new VarNumPatchDataWriter());
        } else {
            FieldType ft = mAttrConf->GetFieldType();
            switch (ft) {
#define MACRO(field_type)                                                                                              \
    case field_type: {                                                                                                 \
        typedef typename config::FieldTypeTraits<field_type>::AttrItemType TT;                                         \
        mPatchDataWriter.reset(new SingleValuePatchDataWriter<TT>());                                                  \
        break;                                                                                                         \
    }
                NUMBER_FIELD_MACRO_HELPER(MACRO);
#undef MACRO
            default:
                assert(false);
                IE_LOG(ERROR, "unsupport field type [%s]", config::FieldConfig::FieldTypeToStr(ft));
                return false;
            }
        }
        return mPatchDataWriter->Init(mAttrConf, mergeIOConfig, mDirectory);
    }
};

// DEFINE_SHARED_PTR(AttributeDataPatcherTyped);
}} // namespace indexlib::partition

#endif //__INDEXLIB_ATTRIBUTE_DATA_PATCHER_TYPED_H
