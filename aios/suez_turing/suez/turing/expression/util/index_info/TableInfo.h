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
#include <stdint.h>
#include <string>

#include "autil/Log.h"

namespace suez {
namespace turing {
class AttributeInfos;
class FieldInfos;
class IndexInfo;
class IndexInfos;
class SummaryInfo;
class CustomizedTableInfo;

class TableInfo {
public:
    TableInfo(const std::string &tableName = "");
    ~TableInfo();
    TableInfo(const TableInfo &tableInfo);

public:
    void setTableName(const std::string &tableName);
    void setTableType(uint32_t type);
    void setTableVersion(int32_t version);
    void setFieldInfos(FieldInfos *fieldInfos);
    void setIndexInfos(IndexInfos *indexInfos);
    void setAttributeInfos(AttributeInfos *attributeInfos);
    void setSummaryInfo(SummaryInfo *queryInfo);
    void setCustomizedTableInfo(CustomizedTableInfo *customizedTableInfo);

    uint32_t getTableType() const { return _tableType; };
    int32_t getTableVersion() const { return _tableVersion; };
    const IndexInfo *getPrimaryKeyIndexInfo() const;
    const IndexInfo *getSubDocPrimaryKeyIndexInfo() const;
    const FieldInfos *getFieldInfos() const { return _fieldInfos; }
    FieldInfos *getFieldInfos() { return _fieldInfos; }
    const IndexInfos *getIndexInfos() const { return _indexInfos; }
    IndexInfos *getIndexInfos() { return _indexInfos; }
    const AttributeInfos *getAttributeInfos() const { return _attributeInfos; }
    AttributeInfos *getAttributeInfos() { return _attributeInfos; }
    const SummaryInfo *getSummaryInfo() const { return _summaryInfo; }
    SummaryInfo *getSummaryInfo() { return _summaryInfo; }
    const CustomizedTableInfo *getCustomizedTableInfo() const { return _customizedTableInfo; }
    CustomizedTableInfo *getCustomizedTableInfo() { return _customizedTableInfo; }
    const std::string &getTableName() const { return _tableName; }
    void setPrimaryKeyAttributeFlag(bool flag) { _hasPrimaryKeyAttribute = flag; }
    bool hasPrimaryKeyAttribute() const { return _hasPrimaryKeyAttribute; }
    void setSubSchemaFlag(bool flag) { _hasSubSchema = flag; }
    bool hasSubSchema() const { return _hasSubSchema; }
    void setSubTableName(const std::string &tableName) { _subTableName = tableName; }
    std::string getSubTableName() { return _subTableName; }

private:
    std::string _tableName;
    uint32_t _tableType;
    int32_t _tableVersion = -1;
    FieldInfos *_fieldInfos;
    IndexInfos *_indexInfos;
    AttributeInfos *_attributeInfos;
    SummaryInfo *_summaryInfo;
    bool _hasPrimaryKeyAttribute;
    bool _hasSubSchema;
    std::string _subTableName;
    CustomizedTableInfo *_customizedTableInfo;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TableInfo> TableInfoPtr;

// for ha3
typedef std::map<std::string, suez::turing::TableInfoPtr> ClusterTableInfoMap;
typedef std::shared_ptr<ClusterTableInfoMap> ClusterTableInfoMapPtr;

class TableMap : public std::map<std::string, TableInfoPtr> {
public:
    const TableInfoPtr getTableInfo(const std::string &tableName) const;
    void addTableInfo(const TableInfoPtr &tableInfoPtr);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TableMap> TableMapPtr;

} // namespace turing
} // namespace suez
