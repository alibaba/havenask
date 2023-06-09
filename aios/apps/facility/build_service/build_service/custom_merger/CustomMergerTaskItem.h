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
#ifndef ISEARCH_BS_CUSTOMMERGERTASKITEM_H
#define ISEARCH_BS_CUSTOMMERGERTASKITEM_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace custom_merger {

class CustomMergerField : public autil::legacy::Jsonizable
{
public:
    enum FieldType { INDEX, ATTRIBUTE };
    CustomMergerField() {}
    CustomMergerField(const std::string& _fieldName, FieldType _fieldType)
        : fieldName(_fieldName)
        , fieldType(_fieldType)
    {
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    std::string fieldName;
    FieldType fieldType;
};

class CustomMergerTaskItem : public autil::legacy::Jsonizable
{
public:
    CustomMergerTaskItem();
    ~CustomMergerTaskItem();
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    uint64_t generateTaskKey() const;

public:
    std::vector<CustomMergerField> fields;
    double cost;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMergerTaskItem);

}} // namespace build_service::custom_merger

#endif // ISEARCH_BS_CUSTOMMERGERTASKITEM_H
