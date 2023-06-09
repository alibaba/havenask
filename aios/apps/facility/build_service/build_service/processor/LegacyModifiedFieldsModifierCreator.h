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

#include "build_service/common_define.h"
#include "build_service/processor/ModifiedFieldsModifier.h"
#include "build_service/util/Log.h"

namespace indexlib { namespace config {
class IndexPartitionSchema;
typedef std::shared_ptr<IndexPartitionSchema> IndexPartitionSchemaPtr;
}} // namespace indexlib::config

namespace build_service { namespace processor {

class LegacyModifiedFieldsModifierCreator
{
public:
    typedef std::map<std::string, fieldid_t> FieldIdMap;

public:
    LegacyModifiedFieldsModifierCreator();
    ~LegacyModifiedFieldsModifierCreator();

public:
    static ModifiedFieldsModifierPtr
    createModifiedFieldsModifier(const std::string& srcFieldName, const std::string& dstFieldName,
                                 const indexlib::config::IndexPartitionSchemaPtr& schema,
                                 FieldIdMap& unknownFieldIdMap);

    static ModifiedFieldsModifierPtr createIgnoreModifier(const std::string& ignoreFieldName,
                                                          const indexlib::config::IndexPartitionSchemaPtr& schema);

private:
    static fieldid_t getFieldId(const std::string& fieldName, const indexlib::config::IndexPartitionSchemaPtr& schema,
                                ModifiedFieldsModifier::SchemaType& schemaType);
    static fieldid_t getUnknowFieldId(const std::string& fieldName, FieldIdMap& unknownFieldIdMap);

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::processor
