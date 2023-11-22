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

#include <map>
#include <memory>
#include <string>

#include "build_service/processor/ModifiedFieldsModifier.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace build_service { namespace processor {

class ModifiedFieldsModifierCreator
{
public:
    typedef std::map<std::string, fieldid_t> FieldIdMap;

public:
    ModifiedFieldsModifierCreator();
    ~ModifiedFieldsModifierCreator();

public:
    static ModifiedFieldsModifierPtr
    createModifiedFieldsModifier(const std::string& srcFieldName, const std::string& dstFieldName,
                                 const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                 FieldIdMap& unknownFieldIdMap);

    static ModifiedFieldsModifierPtr
    createIgnoreModifier(const std::string& ignoreFieldName,
                         const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema);

private:
    static fieldid_t getFieldId(const std::string& fieldName,
                                const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                ModifiedFieldsModifier::SchemaType& schemaType);
    static fieldid_t getUnknowFieldId(const std::string& fieldName, FieldIdMap& unknownFieldIdMap);

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::processor
