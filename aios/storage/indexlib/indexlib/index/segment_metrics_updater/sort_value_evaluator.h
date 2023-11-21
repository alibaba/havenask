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
#pragma once

#include <memory>

#include "autil/ConstStringUtil.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SortValueEvaluator
{
public:
    SortValueEvaluator() : mOffset(0), mFieldId(INVALID_FIELDID), mSupportNull(false) {}

    virtual ~SortValueEvaluator() {}

public:
    void Init(const config::AttributeConfigPtr& attrConfig, index::legacy::Reference* refer)
    {
        assert(attrConfig);
        assert(refer);

        mAttrConvertor.reset(common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
        assert(mAttrConvertor);

        mOffset = refer->GetOffset();
        const auto& fieldConfig = attrConfig->GetFieldConfig();
        assert(fieldConfig);
        mFieldId = fieldConfig->GetFieldId();
        mSupportNull = fieldConfig->IsEnableNullField();
    }

    virtual void Evaluate(const document::AttributeDocumentPtr& attrDoc, index::legacy::DocInfo* docInfo) = 0;

protected:
    common::AttributeConvertorPtr mAttrConvertor;
    size_t mOffset;
    fieldid_t mFieldId;
    bool mSupportNull;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortValueEvaluator);

template <typename T>
class SortValueEvaluatorTyped : public SortValueEvaluator
{
public:
    SortValueEvaluatorTyped() : mDefaultValue(T()), mTemp(T())
    {
        if (typeid(T) != typeid(std::string)) {
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
            memset(&mDefaultValue, 0, sizeof(mDefaultValue));
#pragma GCC diagnostic pop
#endif
        }
    }
    ~SortValueEvaluatorTyped() {}

    void InitForTemplate(config::AttributeConfigPtr& attrConfig)
    {
        mAttrConvertor.reset(common::AttributeConvertorFactory ::GetInstance()->CreateAttrConvertor(attrConfig));
        assert(mAttrConvertor);
        auto fieldConfig = attrConfig->GetFieldConfig();
        assert(fieldConfig);
        mFieldId = fieldConfig->GetFieldId();
        mSupportNull = fieldConfig->IsEnableNullField();
    }

    void Evaluate(const document::AttributeDocumentPtr& attrDoc, index::legacy::DocInfo* docInfo) override
    {
        assert(attrDoc);
        bool isNull = false;
        const autil::StringView& field = attrDoc->GetField(mFieldId, isNull);

        uint8_t* buffer = docInfo->Get(mOffset);
        if (mSupportNull) {
            memcpy(buffer, &isNull, sizeof(bool));
            buffer = buffer + sizeof(bool);
        }
        T& value = *(T*)buffer;

        if (likely(!field.empty())) {
            assert(mAttrConvertor);
            common::AttrValueMeta meta = mAttrConvertor->Decode(field);
            value = *(T*)meta.data.data();
        } else {
            value = mDefaultValue;
        }
    }

    const T& Evaluate(const document::AttributeDocumentPtr& attrDoc, bool* isNull);

private:
    const T& ConvertString(const autil::StringView& field);

private:
    T mDefaultValue;
    T mTemp;
};

template <typename T>
const T& SortValueEvaluatorTyped<T>::Evaluate(const document::AttributeDocumentPtr& attrDoc, bool* isNull)
{
    assert(attrDoc);
    const autil::StringView& field = attrDoc->GetField(mFieldId, *isNull);
    return ConvertString(field);
}

template <typename T>
const T& SortValueEvaluatorTyped<T>::ConvertString(const autil::StringView& field)
{
    if (likely(!field.empty())) {
        assert(mAttrConvertor);
        common::AttrValueMeta meta = mAttrConvertor->Decode(field);
        return *(T*)meta.data.data();
    }
    return mDefaultValue;
}

template <>
inline const std::string& SortValueEvaluatorTyped<std::string>::ConvertString(const autil::StringView& field)
{
    if (likely(!field.empty())) {
        assert(mAttrConvertor);
        mAttrConvertor->DecodeLiteralField(field, mTemp);
        return mTemp;
    }
    return mDefaultValue;
}
}} // namespace indexlib::index
