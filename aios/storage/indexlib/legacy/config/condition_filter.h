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
#include "indexlib/util/KeyValueMap.h"

namespace indexlib::config {
class ConditionFilterImpl;
typedef std::shared_ptr<ConditionFilterImpl> ConditionFilterImplPtr;
} // namespace indexlib::config
namespace indexlib { namespace config {

class ConditionFilter : public autil::legacy::Jsonizable
{
public:
    ConditionFilter();
    ~ConditionFilter();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check(const AttributeSchemaPtr& attributeSchema) const;
    const std::string& GetType() const;
    const std::string& GetValue() const;
    const std::string& GetValueType() const;
    const std::string& GetFieldName() const;
    const std::string& GetFunctionName() const;
    const util::KeyValueMap& GetFunctionParam() const;
    void AssertEqual(const ConditionFilter& other) const;

public:
    bool operator==(const ConditionFilter& other) const;

private:
    ConditionFilterImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ConditionFilter> ConditionFilterPtr;
}} // namespace indexlib::config
