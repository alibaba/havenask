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

#include "indexlib/indexlib.h"
#include "suez/service/Service.pb.h"

namespace indexlibv2::config {
class ITabletSchema;
}
namespace indexlib::config {
class IndexPartitionSchema;
}

namespace suez {

class SchemaUtil {
public:
    static ErrorInfo getPbSchema(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                                 const std::string &attr,
                                 ResultSchema &resultSchema);

    static ErrorInfo getJsonSchema(const std::shared_ptr<indexlibv2::config::ITabletSchema> &indexPartition,
                                   ResultSchema &resultSchema);

    static ErrorInfo convertFieldType(const FieldType &fieldType, ValueType &valueType);

    static void tableTypeToStr(const indexlib::TableType &tableType, std::string &tableTypeStr);

private:
    // TODO: when indexlibv2 support pack attribute, remove this function
    static ErrorInfo getPbSchema(const std::shared_ptr<indexlib::config::IndexPartitionSchema> &indexPartitionSchemaPtr,
                                 const std::string &targetAttr,
                                 ResultSchema &resultSchema);
};
} // namespace suez
