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

#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/common/catalog/CatalogDef.h"

namespace sql {

class KhronosTableConverter {
public:
    static bool fillKhronosTable(const iquan::TableModel &tm,
                                 const std::string &dbName,
                                 const iquan::LocationSign &locationSign,
                                 iquan::CatalogDefs &catalogDefs);
    static bool isKhronosTable(const iquan::TableDef &td);
    static bool fillKhronosLogicTableV3(const iquan::TableModel &tm,
                                        const std::string &dbName,
                                        const iquan::LocationSign &locationSign,
                                        iquan::CatalogDefs &catalogDefs);
    static bool fillKhronosLogicTableV2(const iquan::TableModel &tm,
                                        const std::string &dbName,
                                        const iquan::LocationSign &locationSign,
                                        iquan::CatalogDefs &catalogDefs);

private:
    static bool fillKhronosMetaTable(const iquan::TableModel &tm,
                                     const std::string &originDbName,
                                     const iquan::LocationSign &locationSign,
                                     iquan::CatalogDefs &catalogDefs);
    static bool doFillKhronosMetaTable(const std::string &tableName,
                                       const std::string &dbName,
                                       const std::string &catalogName,
                                       const iquan::TableModel &tm,
                                       const iquan::LocationSign &locationSign,
                                       iquan::CatalogDefs &catalogDefs);
    static void fillKhronosBuiltinFields(iquan::TableModel &tableModel);
    static bool fillKhronosDataFields(const std::string &khronosType,
                                      const std::string &valueSuffix,
                                      const std::string &fieldsStr,
                                      const std::vector<iquan::FieldDef> fields,
                                      iquan::TableDef &tableDef);
};

} // namespace sql
