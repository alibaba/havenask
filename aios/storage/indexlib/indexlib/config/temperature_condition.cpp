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
#include "indexlib/config/temperature_condition.h"

#include "indexlib/config/impl/temperature_condition_impl.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, TemperatureCondition);

TemperatureCondition::TemperatureCondition() : mImpl(new TemperatureConditionImpl()) {}

TemperatureCondition::~TemperatureCondition() {}

void TemperatureCondition::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

void TemperatureCondition::Check(const AttributeSchemaPtr& attributeSchema,
                                 std::map<std::string, std::string>& field2Fun) const
{
    mImpl->Check(attributeSchema, field2Fun);
}

string TemperatureCondition::GetProperty() const { return mImpl->GetProperty(); }

const std::vector<ConditionFilterPtr>& TemperatureCondition::GetFilters() const { return mImpl->GetFilters(); }

void TemperatureCondition::AssertEqual(const TemperatureCondition& condition) const
{
    mImpl->AssertEqual(*condition.mImpl);
}

bool TemperatureCondition::operator==(const TemperatureCondition& other) const { return *mImpl == *other.mImpl; }
}} // namespace indexlib::config
