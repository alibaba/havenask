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
#include "indexlib/index/segment_metrics_updater/filter_matcher_creator.h"

#include "indexlib/config/attribute_config.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, FilterMatcherCreator);

FilterMatcherCreator::FilterMatcherCreator() {}

FilterMatcherCreator::~FilterMatcherCreator() {}

FilterMatcherPtr FilterMatcherCreator::Create(const config::AttributeSchemaPtr& attrSchema,
                                              const config::ConditionFilterPtr& filter, int32_t idx)
{
    string fieldName = filter->GetFieldName();
    config::AttributeConfigPtr config = attrSchema->GetAttributeConfig(fieldName);
    assert(config);
    FieldType fieldType = config->GetFieldType();
    string type = filter->GetType();
    string valueType = filter->GetValueType();
    FilterMatcherPtr matcher;
    if (type == "Range") {
        matcher.reset(CreateFilter<RangeFilterMatcher>(fieldType, valueType));
    } else if (type == "Equal") {
        matcher.reset(CreateFilter<EqualFilterMatcher>(fieldType, valueType));
    } else {
        IE_LOG(ERROR, "type [%s] is invalid, it should be Range or Equal", type.c_str());
        return FilterMatcherPtr();
    }
    if (!matcher) {
        IE_LOG(ERROR, "matcher create failed, fieldType[%d] valueType[%s]", fieldType, valueType.c_str());
        return FilterMatcherPtr();
    }
    if (!matcher->Init(attrSchema, filter, idx)) {
        IE_LOG(ERROR, "matcher init failed");
        return FilterMatcherPtr();
    }
    return matcher;
}

bool FilterMatcherCreator::AddValue(FilterMatcherPtr& matcher, const config::ConditionFilterPtr& filter, int32_t idx)
{
    if (!matcher->AddValue(filter->GetValue(), idx)) {
        IE_LOG(ERROR, "invalid filter value [%s]", filter->GetValue().c_str());
        return false;
    }
    return true;
}

FilterMatcherPtr FilterMatcherCreator::Create(const config::AttributeSchemaPtr& attrSchema,
                                              const config::ConditionFilterPtr& filter,
                                              const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                                              int32_t idx)
{
    FilterMatcherPtr matcher;
    config::AttributeConfigPtr config = attrSchema->GetAttributeConfig(filter->GetFieldName());
    assert(config);
    FieldType fieldType = config->GetFieldType();
    string type = filter->GetType();
    string valueType = filter->GetValueType();
    if (type == "Range") {
        matcher.reset(CreateFilter<RangeFilterMatcher>(fieldType, valueType));
    } else if (type == "Equal") {
        matcher.reset(CreateFilter<EqualFilterMatcher>(fieldType, valueType));
    } else {
        IE_LOG(ERROR, "type [%s] is invalid, it should be Range or Equal", type.c_str());
        return FilterMatcherPtr();
    }
    if (!matcher) {
        IE_LOG(ERROR, "matcher create failed, fieldType[%d] valueType[%s]", fieldType, valueType.c_str());
        return FilterMatcherPtr();
    }
    if (!matcher->InitForMerge(attrSchema, filter, attrReaders, idx)) {
        IE_LOG(ERROR, "matcher init failed");
        return FilterMatcherPtr();
    }
    return matcher;
}
}} // namespace indexlib::index
