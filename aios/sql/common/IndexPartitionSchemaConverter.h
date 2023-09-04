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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/summary_schema.h"

namespace iquan {
class FieldDef;
class FieldTypeDef;
class TableDef;

;
} // namespace iquan

namespace indexlibv2::config {
class ITabletSchema;
}

namespace sql {

class IndexPartitionSchemaConverter {
public:
    IndexPartitionSchemaConverter();
    ~IndexPartitionSchemaConverter();
    IndexPartitionSchemaConverter(const IndexPartitionSchemaConverter &) = delete;
    IndexPartitionSchemaConverter &operator=(const IndexPartitionSchemaConverter &) = delete;

public:
    static bool convert(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                        iquan::TableDef &tableDef);

private:
    // indexlibv2
    static bool convertV2(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                          iquan::TableDef &tableDef);

    static bool convertNormalTable(const indexlib::config::IndexPartitionSchemaPtr &schema,
                                   const std::string &tableType,
                                   iquan::TableDef &tableDef);
    static bool convertNormalTable(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                                   const std::string &tableType,
                                   iquan::TableDef &tableDef);
    static bool convertNonInvertedTable(const indexlib::config::IndexPartitionSchemaPtr &schema,
                                        const std::string &tableType,
                                        iquan::TableDef &tableDef);
    static bool
    convertNonInvertedTable(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                            const std::string &tableType,
                            iquan::TableDef &tableDef);

    static bool isKhronosTable(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);
    static bool
    convertKhronosTable(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                        iquan::TableDef &tableDef);
    static bool convertLegacyKhronosSchema(const indexlib::config::IndexPartitionSchemaPtr &schema,
                                           iquan::TableDef &tableDef);
    static void convertIndexFieldConfig(const indexlib::config::IndexSchemaPtr &indexSchema,
                                        const std::string &tableType,
                                        iquan::FieldDef &fieldDef);
    static void
    convertIndexFieldConfig(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                            const std::string &tableType,
                            iquan::FieldDef &fieldDef);
    static bool convertFields(const indexlib::config::FieldSchemaPtr &fieldSchema,
                              const indexlib::config::IndexSchemaPtr &indexSchema,
                              const indexlib::config::AttributeSchemaPtr &attrSchema,
                              const indexlib::config::SummarySchemaPtr &summarySchema,
                              const std::string &tableType,
                              std::vector<iquan::FieldDef> &fields);
    static bool convertFields(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                              const std::string &tableType,
                              std::vector<iquan::FieldDef> &fields);
    static void convertFieldType(const std::string &fieldType,
                                 bool isMultiValue,
                                 iquan::FieldTypeDef &fieldTypeDef);
    static inline bool isCustomizedFieldTypeToDoubleArray(const std::string &fieldType);
    static inline bool isCustomizedFieldTypeToString(const std::string &fieldType);
    static inline bool isNormalPkIndex(const std::string &indexType);


private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexPartitionSchemaConverter> IndexPartitionSchemaConverterPtr;
} // namespace sql
