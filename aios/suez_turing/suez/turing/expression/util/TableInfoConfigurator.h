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

#include <string>

#include "autil/Log.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/customized_table_config.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/schema_modify_operation.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_application.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace suez {
namespace turing {
class AttributeInfos;
class CustomizedTableInfo;
class FieldInfo;
class FieldInfos;
class IndexInfo;
class IndexInfos;
class SummaryInfo;

class TableInfoConfigurator {
public:
    TableInfoConfigurator();
    ~TableInfoConfigurator();

public:
    static TableInfoPtr createFromSchema(const indexlib::config::IndexPartitionSchemaPtr &indexPartitionSchemaPtr,
                                         int32_t version = -1);
    static TableInfoPtr createFromSchema(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                                         int32_t version = -1);
    static TableInfoPtr createFromString(const std::string &configStr);
    static TableInfoPtr createFromFile(const std::string &filePath);
    static TableInfoPtr createFromIndexApp(const std::string &mainTable,
                                           const indexlib::partition::IndexApplicationPtr &indexApp);

private:
    static TableInfoPtr createTableInfo(const indexlib::config::IndexPartitionSchemaPtr &indexPartitionSchemaPtr,
                                        int32_t version = -1);
    static TableInfoPtr createTableInfo(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                                        int32_t version = -1);
    static FieldInfos *
    transToFieldInfos(const std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> &fieldConfigs);
    static std::unique_ptr<SummaryInfo> transToSummaryInfo(indexlib::config::SummarySchemaPtr summarySchemaPtr);
    static std::unique_ptr<SummaryInfo>
    transToSummaryInfo(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);
    static std::unique_ptr<CustomizedTableInfo>
    transToCustomizedTableInfo(indexlib::config::CustomizedTableConfigPtr configPtr);
    static std::unique_ptr<AttributeInfos>
    transToAttributeInfos(FieldInfos *fieldInfos, indexlib::config::AttributeSchemaPtr attributeSchemaPtr);
    static std::unique_ptr<AttributeInfos>
    transToAttributeInfos(FieldInfos *fieldInfos, const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);
    static std::unique_ptr<IndexInfo> transToIndexInfo(indexlib::config::IndexConfigPtr indexConfigPtr);
    static std::unique_ptr<IndexInfos> transToIndexInfos(indexlib::config::IndexSchemaPtr indexSchemaPtr);
    static std::unique_ptr<IndexInfos>
    transToIndexInfos(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);
    static FieldInfo *transToFieldInfo(const std::shared_ptr<indexlibv2::config::FieldConfig> &fieldConfigPtr);
    static void mergeTableInfo(TableInfoPtr &subTableInfo, TableInfoPtr &mainTableInfo);
    static void mergeFieldInfos(FieldInfos *subFieldInfos, FieldInfos *mainFieldInfos);
    static void mergeAttributeInfos(AttributeInfos *subAttributeInfos, AttributeInfos *mainAttributeInfos);
    static void mergeIndexInfos(IndexInfos *subIndexInfos, IndexInfos *mainIndexInfos);
    static void mergeSummaryInfo(SummaryInfo *subSummaryInfos, SummaryInfo *mainSummaryInfos);
    static bool isKhronosTableType(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
