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
#include "indexlib/config/condition_filter.h"

#include "indexlib/config/impl/condition_filter_impl.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, ConditionFilter);

ConditionFilter::ConditionFilter() : mImpl(new ConditionFilterImpl()) {}
ConditionFilter::~ConditionFilter() {}

void ConditionFilter::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }
void ConditionFilter::Check(const AttributeSchemaPtr& attributeSchema) const { mImpl->Check(attributeSchema); }

const std::string& ConditionFilter::GetType() const { return mImpl->GetType(); }

const std::string& ConditionFilter::GetValue() const { return mImpl->GetValue(); }

const std::string& ConditionFilter::GetValueType() const { return mImpl->GetValueType(); }

const std::string& ConditionFilter::GetFieldName() const { return mImpl->GetFieldName(); }

const std::string& ConditionFilter::GetFunctionName() const { return mImpl->GetFunctionName(); }

const util::KeyValueMap& ConditionFilter::GetFunctionParam() const { return mImpl->GetFunctionParam(); }

void ConditionFilter::AssertEqual(const ConditionFilter& other) const { return mImpl->AssertEqual(*other.mImpl); }

bool ConditionFilter::operator==(const ConditionFilter& other) const { return *mImpl == *other.mImpl; }
}} // namespace indexlib::config
