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
#ifndef __INDEXLIB_TEMPERATURE_CONDITION_IMPL_H
#define __INDEXLIB_TEMPERATURE_CONDITION_IMPL_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/condition_filter.h"

namespace indexlib { namespace config {

class TemperatureConditionImpl : public autil::legacy::Jsonizable
{
public:
    TemperatureConditionImpl();
    ~TemperatureConditionImpl();

    TemperatureConditionImpl(const TemperatureConditionImpl&) = delete;
    TemperatureConditionImpl& operator=(const TemperatureConditionImpl&) = delete;
    TemperatureConditionImpl(TemperatureConditionImpl&&) = delete;
    TemperatureConditionImpl& operator=(TemperatureConditionImpl&&) = delete;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check(const AttributeSchemaPtr& attributeSchema, std::map<std::string, std::string>& field2Fun) const;
    const std::string& GetProperty() const;
    const std::vector<ConditionFilterPtr>& GetFilters() const;
    void AssertEqual(const TemperatureConditionImpl& other) const;

public:
    bool operator==(const TemperatureConditionImpl& other) const;

private:
    void CheckFunc(std::map<std::string, std::string>& field2Fun) const;

private:
    std::string mProperty;
    std::vector<ConditionFilterPtr> mFilters;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TemperatureConditionImpl);
}} // namespace indexlib::config

#endif //__INDEXLIB_TEMPERATURE_CONDITION_IMPL_H
