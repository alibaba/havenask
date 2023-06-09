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
#ifndef __INDEXLIB_CONDITION_FILTER_FUNCTION_CHECKER_H
#define __INDEXLIB_CONDITION_FILTER_FUNCTION_CHECKER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/impl/condition_filter_impl.h"

namespace indexlib { namespace config {

class ConditionFilterFunctionChecker
{
public:
    ConditionFilterFunctionChecker();
    ~ConditionFilterFunctionChecker();

    ConditionFilterFunctionChecker(const ConditionFilterFunctionChecker&) = delete;
    ConditionFilterFunctionChecker& operator=(const ConditionFilterFunctionChecker&) = delete;
    ConditionFilterFunctionChecker(ConditionFilterFunctionChecker&&) = delete;
    ConditionFilterFunctionChecker& operator=(ConditionFilterFunctionChecker&&) = delete;

public:
    void init(const AttributeSchemaPtr& attrSchema);
    void CheckFunction(const ConditionFilterImpl::FilterFunctionConfig& functionConfig, const std::string& fieldName,
                       const std::string& valueType);
    void CheckValue(const std::string& type, const std::string& fieldName, const std::string& strValue,
                    const std::string& valueType);

private:
    void CheckFuncValid(const ConditionFilterImpl::FilterFunctionConfig& functionConfig, const std::string& fieldName,
                        const std::string& valueType);

private:
    std::set<std::string> mBuildInFunc;
    AttributeSchemaPtr mAttrSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ConditionFilterFunctionChecker);
}} // namespace indexlib::config

#endif //__INDEXLIB_CONDITION_FILTER_FUNCTION_CHECKER_H
