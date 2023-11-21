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

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/condition_filter.h"
#include "indexlib/index/segment_metrics_updater/equal_filter_matcher.h"
#include "indexlib/index/segment_metrics_updater/filter_matcher.h"
#include "indexlib/index/segment_metrics_updater/range_filter_matcher.h"

namespace indexlib { namespace index {

class FilterMatcherCreator
{
public:
    FilterMatcherCreator();
    ~FilterMatcherCreator();

public:
    static FilterMatcherPtr Create(const config::AttributeSchemaPtr& attrSchema,
                                   const config::ConditionFilterPtr& filter, int32_t idx);

    static FilterMatcherPtr Create(const config::AttributeSchemaPtr& attrSchema,
                                   const config::ConditionFilterPtr& filter,
                                   const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders, int32_t idx);

    static bool AddValue(FilterMatcherPtr& matcher, const config::ConditionFilterPtr& filter, int32_t idx);

private:
    template <template <typename X, typename Y> class ClassType>
    static FilterMatcher* CreateFilter(const FieldType& fieldType, const std::string& rangeType);
    template <template <typename X, typename Y> class ClassTyped, typename Y>
    static FilterMatcher* InnerCreate(const FieldType& fieldType);

private:
    IE_LOG_DECLARE();
};

template <template <typename X, typename Y> class ClassType>
FilterMatcher* FilterMatcherCreator::CreateFilter(const FieldType& fieldType, const std::string& valueType)
{
    FieldType valueFieldType = config::FieldConfig::StrToFieldType(valueType);
    if (valueFieldType == ft_int64) {
        return InnerCreate<ClassType, int64_t>(fieldType);
    } else if (valueFieldType == ft_uint64) {
        return InnerCreate<ClassType, uint64_t>(fieldType);
    } else if (valueFieldType == ft_double) {
        return InnerCreate<ClassType, double>(fieldType);
    } else if (valueFieldType == ft_string) {
        if (fieldType != ft_string) {
            return nullptr;
        }
        // only support equal filter attribute and value type is both string
        return new ClassType<std::string, std::string>();
    }
    return nullptr;
}

template <template <typename X, typename Y> class ClassTyped, typename Y>
FilterMatcher* FilterMatcherCreator::InnerCreate(const FieldType& fieldType)
{
    switch (fieldType) {
    case ft_int8:
        return new ClassTyped<int8_t, Y>();
    case ft_uint8:
        return new ClassTyped<uint8_t, Y>();
    case ft_int16:
        return new ClassTyped<int16_t, Y>();
    case ft_uint16:
        return new ClassTyped<uint16_t, Y>();
    case ft_int32:
        return new ClassTyped<int32_t, Y>();
    case ft_uint32:
        return new ClassTyped<uint32_t, Y>();
    case ft_int64:
        return new ClassTyped<int64_t, Y>();
    case ft_uint64:
        return new ClassTyped<uint64_t, Y>();
    case ft_float:
        return new ClassTyped<float, Y>();
    case ft_double:
        return new ClassTyped<double, Y>();
    default:
        return nullptr;
    }
    return nullptr;
}

DEFINE_SHARED_PTR(FilterMatcherCreator);
}} // namespace indexlib::index
