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
#ifndef __INDEXLIB_TEMPERATURE_CONDITION_H
#define __INDEXLIB_TEMPERATURE_CONDITION_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/condition_filter.h"

DECLARE_REFERENCE_CLASS(config, TemperatureConditionImpl);

namespace indexlib { namespace config {

class TemperatureCondition : public autil::legacy::Jsonizable
{
public:
    TemperatureCondition();
    ~TemperatureCondition();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check(const AttributeSchemaPtr& attributeSchema, std::map<std::string, std::string>& field2Fun) const;
    std::string GetProperty() const;
    const std::vector<ConditionFilterPtr>& GetFilters() const;
    void AssertEqual(const TemperatureCondition& condition) const;

public:
    bool operator==(const TemperatureCondition& other) const;

private:
    TemperatureConditionImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TemperatureCondition);
}} // namespace indexlib::config

#endif //__INDEXLIB_TEMPERATURE_CONDITION_H
