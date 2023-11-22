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

namespace indexlib { namespace config {

class ConditionFilterImpl : public autil::legacy::Jsonizable
{
public:
    struct FilterFunctionConfig : public autil::legacy::Jsonizable {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("functionName", funcName, funcName);
            json.Jsonize("param", param, param);
        }
        bool operator==(const FilterFunctionConfig& other) const
        {
            return funcName == other.funcName && param == other.param;
        }
        bool operator!=(const FilterFunctionConfig& other) const { return !(*this == other); }
        std::string funcName;
        util::KeyValueMap param;
    };

public:
    ConditionFilterImpl();
    ~ConditionFilterImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check(const AttributeSchemaPtr& attributeSchema) const;
    const std::string& GetType() const { return mType; }
    const std::string& GetValue() const { return mValue; }
    const std::string& GetValueType() const { return mValueType; }
    const std::string& GetFieldName() const { return mFieldName; }
    const std::string& GetFunctionName() const { return mFunction.funcName; }
    const util::KeyValueMap& GetFunctionParam() const { return mFunction.param; }
    void AssertEqual(const ConditionFilterImpl& other);

public:
    bool operator==(const ConditionFilterImpl& other);

private:
    std::string mType;
    std::string mFieldName;
    FilterFunctionConfig mFunction;
    std::string mValue;
    std::string mValueType;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ConditionFilterImpl> ConditionFilterImplPtr;
}} // namespace indexlib::config
