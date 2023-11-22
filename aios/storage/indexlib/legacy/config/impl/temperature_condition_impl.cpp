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
#include "indexlib/config/impl/temperature_condition_impl.h"

#include "indexlib/config/config_define.h"
#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, TemperatureConditionImpl);

TemperatureConditionImpl::TemperatureConditionImpl() {}

TemperatureConditionImpl::~TemperatureConditionImpl() {}

void TemperatureConditionImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("property", mProperty);
    json.Jsonize("filters", mFilters, mFilters);
}

void TemperatureConditionImpl::Check(const AttributeSchemaPtr& attributeSchema, map<string, string>& field2Fun) const
{
    if (mProperty != "HOT" && mProperty != "WARM" && mProperty != "COLD") {
        INDEXLIB_FATAL_ERROR(Schema, "field Property [%s] can only be Hot,Warm,Cold", mProperty.c_str());
    }
    for (auto& filter : mFilters) {
        filter->Check(attributeSchema);
    }
    // not support same field set diff function
    CheckFunc(field2Fun);
}

void TemperatureConditionImpl::CheckFunc(map<string, string>& field2Fun) const
{
    for (auto& filter : mFilters) {
        auto iter = field2Fun.find(filter->GetFieldName());
        if (iter == field2Fun.end()) {
            field2Fun.insert(make_pair(filter->GetFieldName(), filter->GetFunctionName()));
        } else {
            if (filter->GetFunctionName() != iter->second) {
                INDEXLIB_FATAL_ERROR(Schema, "field [%s] function set not equal", iter->first.c_str());
            }
        }
    }
}

const std::string& TemperatureConditionImpl::GetProperty() const { return mProperty; }

const std::vector<ConditionFilterPtr>& TemperatureConditionImpl::GetFilters() const { return mFilters; }

void TemperatureConditionImpl::AssertEqual(const TemperatureConditionImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mProperty, other.mProperty, "filter Property not equal");
    IE_CONFIG_ASSERT_EQUAL(mFilters.size(), other.mFilters.size(), "filter size not equal");
    for (size_t i = 0; i < mFilters.size(); i++) {
        mFilters[i]->AssertEqual((*other.mFilters[i]));
    }
}

bool TemperatureConditionImpl::operator==(const TemperatureConditionImpl& other) const
{
    if (mProperty != other.mProperty) {
        return false;
    }
    if (mFilters.size() != mFilters.size()) {
        return false;
    }
    for (size_t i = 0; i < mFilters.size(); i++) {
        if (*mFilters[i] == *(other.mFilters[i])) {
            continue;
        }
        return false;
    }
    return true;
}

}} // namespace indexlib::config
