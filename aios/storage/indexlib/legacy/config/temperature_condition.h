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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/condition_filter.h"

namespace indexlib::config {
class TemperatureConditionImpl;
typedef std::shared_ptr<TemperatureConditionImpl> TemperatureConditionImplPtr;
} // namespace indexlib::config

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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TemperatureCondition> TemperatureConditionPtr;
}} // namespace indexlib::config
