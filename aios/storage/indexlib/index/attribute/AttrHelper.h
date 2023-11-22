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

#include "indexlib/base/Status.h"
#include "indexlib/base/proto/query.pb.h"
#include "indexlib/index/attribute/AttributeIteratorBase.h"
#include "indexlib/index/common/AttrTypeTraits.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReference.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"

namespace indexlib::index {

class AttrHelper
{
public:
    static void FillAttrInfo(const std::vector<std::string>& attrs,
                             indexlibv2::base::PartitionResponse& partitionResponse)
    {
        if (partitionResponse.rows_size() > 0) {
            auto row = partitionResponse.rows(0);
            for (size_t i = 0; i < attrs.size(); i++) {
                auto field = partitionResponse.mutable_attrinfo()->add_fields();
                field->set_attrname(attrs[i]);
                field->set_type(row.attrvalues(i).type());
            }
        }
    }

    static bool GetAttributeValue(const autil::StringView& value,
                                  const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& formatter,
                                  const std::string& name, indexlibv2::base::AttrValue& attrValue)
    {
        auto attrConfig = formatter->GetAttributeConfig(name);
        auto attrRef = formatter->GetAttributeReference(name);
        return AttrHelper::GetAttributeValue(attrRef, value.data(), attrConfig, attrValue);
    }

    static bool GetAttributeValue(const autil::StringView& value,
                                  const indexlibv2::index::PackAttributeFormatter* formatter, const std::string& name,
                                  indexlibv2::base::AttrValue& attrValue)
    {
        auto attrConfig = formatter->GetAttributeConfig(name);
        auto attrRef = formatter->GetAttributeReference(name);
        return AttrHelper::GetAttributeValue(attrRef, value.data(), attrConfig, attrValue);
    }

    template <typename Fetcher, typename Src>
    static bool GetAttributeValue(Fetcher* const attrFetcher, const Src attrIndex,
                                  const std::shared_ptr<indexlibv2::index::AttributeConfig>& attrConfig,
                                  indexlibv2::base::AttrValue& rattrvalue)
    {
        auto fieldType = attrConfig->GetFieldType();
        auto isMultiValue = attrConfig->IsMultiValue();
        auto isLengthFixed = attrConfig->IsLengthFixed();
        return GetAttributeValue(attrFetcher, attrIndex, fieldType, isMultiValue, isLengthFixed, rattrvalue);
    }

    template <typename Fetcher, typename Src>
    static bool GetAttributeValue(Fetcher* const attrFetcher, const Src attrIndex, const FieldType fieldType,
                                  const bool isMultiValue, const bool isLengthFixed,
                                  indexlibv2::base::AttrValue& rattrvalue);

    static std::vector<std::string> ValidateAttrs(const std::vector<std::string>& attrs,
                                                  const std::vector<std::string>& inputAttrs);

public:
    template <typename T>
    static bool FetchTypeValue(indexlibv2::index::AttributeIteratorBase* reader, docid_t docid, T& value);

    template <typename T>
    static bool FetchTypeValue(indexlibv2::index::AttributeReference* attrRef, const char* baseAddr, T& value);
};

#define FILL_ATTRIBUTE(ft, strValueType, attrFetcher, attrIndex)                                                       \
    case ft: {                                                                                                         \
        attrValue.set_type(AttrTypeTraits2<ft>::valueType);                                                            \
        typedef indexlib::index::FieldTypeTraits<ft>::AttrItemType InnerType;                                          \
        if (isMultiValue) {                                                                                            \
            typedef autil::MultiValueType<InnerType> MultiValueType;                                                   \
            MultiValueType multiValue;                                                                                 \
            if (FetchTypeValue<MultiValueType>(attrFetcher, attrIndex, multiValue)) {                                  \
                AttrTypeTraits2<ft>::AttrMultiType multiTypeValue;                                                     \
                for (int i = 0; i != multiValue.size(); ++i) {                                                         \
                    multiTypeValue.add_value(multiValue[i]);                                                           \
                }                                                                                                      \
                *attrValue.mutable_multi_##strValueType() = multiTypeValue;                                            \
                return true;                                                                                           \
            }                                                                                                          \
        } else {                                                                                                       \
            InnerType value;                                                                                           \
            if (FetchTypeValue<InnerType>(attrFetcher, attrIndex, value)) {                                            \
                attrValue.set_##strValueType(value);                                                                   \
                return true;                                                                                           \
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }

template <typename Fetcher, typename Index>
inline bool AttrHelper::GetAttributeValue(Fetcher* const attrFetcher, const Index attrIndex, const FieldType fieldType,
                                          const bool isMultiValue, const bool isLengthFixed,
                                          indexlibv2::base::AttrValue& attrValue)
{
    switch (fieldType) {
        FILL_ATTRIBUTE(ft_int8, int32_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_uint8, uint32_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_int16, int32_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_uint16, uint32_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_int32, int32_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_uint32, uint32_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_int64, int64_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_uint64, uint64_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_float, float_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_double, double_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_location, double_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_line, double_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_polygon, double_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_time, uint32_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_timestamp, uint64_value, attrFetcher, attrIndex)
        FILL_ATTRIBUTE(ft_date, int32_value, attrFetcher, attrIndex)

    case ft_string:
    default:
        attrValue.set_type(AttrTypeTraits2<ft_string>::valueType);
        if (isMultiValue) {
            autil::MultiString multiValue;
            if (FetchTypeValue<autil::MultiString>(attrFetcher, attrIndex, multiValue)) {
                for (int i = 0; i != multiValue.size(); ++i) {
                    attrValue.mutable_multi_bytes_value()->add_value(multiValue[i].data(), multiValue[i].size());
                }
                return true;
            }
        } else {
            autil::MultiChar singleValue;
            if (FetchTypeValue<autil::MultiChar>(attrFetcher, attrIndex, singleValue)) {
                attrValue.set_bytes_value(std::string(singleValue.data(), singleValue.size()));
                return true;
            }
        }
        break;
    }
    return false;
#undef FILL_ATTRIBUTE
}

template <typename T>
inline bool AttrHelper::FetchTypeValue(indexlibv2::index::AttributeReference* attrRef, const char* baseAddr, T& value)
{
    typedef indexlibv2::index::AttributeReferenceTyped<T> AttrReference;
    AttrReference* newAttrRef = static_cast<AttrReference*>(attrRef);
    return newAttrRef->GetValue(baseAddr, value);
}

} // namespace indexlib::index
