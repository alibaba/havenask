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
#include "indexlib/config/impl/condition_filter_impl.h"

#include "indexlib/config/condition_filter_function_checker.h"
#include "indexlib/config/config_define.h"
#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, ConditionFilterImpl);

ConditionFilterImpl::ConditionFilterImpl() {}

ConditionFilterImpl::~ConditionFilterImpl() {}

void ConditionFilterImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("type", mType);
    json.Jsonize("field_name", mFieldName);
    json.Jsonize("function", mFunction, mFunction);
    json.Jsonize("value", mValue);
    json.Jsonize("value_type", mValueType);
}

void ConditionFilterImpl::Check(const AttributeSchemaPtr& attributeSchema) const
{
    ConditionFilterFunctionChecker checker;
    checker.init(attributeSchema);
    checker.CheckValue(mType, mFieldName, mValue, mValueType);
    checker.CheckFunction(mFunction, mFieldName, mValueType);
}
void ConditionFilterImpl::AssertEqual(const ConditionFilterImpl& other)
{
    IE_CONFIG_ASSERT_EQUAL(mType, other.mType, "filter type not equal");
    IE_CONFIG_ASSERT_EQUAL(mFieldName, other.mFieldName, "field name not equal");
    IE_CONFIG_ASSERT_EQUAL(mFunction, other.mFunction, "function not equal");
    IE_CONFIG_ASSERT_EQUAL(mValue, other.mValue, "value not equal");
    IE_CONFIG_ASSERT_EQUAL(mValueType, other.mValueType, "value type not equal");
}

bool ConditionFilterImpl::operator==(const ConditionFilterImpl& other)
{
    if (mType != other.mType) {
        return false;
    }
    if (mFieldName != other.mFieldName) {
        return false;
    }
    if (mFunction != other.mFunction) {
        return false;
    }
    if (mValue != other.mValue) {
        return false;
    }
    if (mValueType != other.mValueType) {
        return false;
    }
    return true;
}

}} // namespace indexlib::config
