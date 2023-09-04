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
#include "suez/turing/expression/util/TableInfo.h"

#include <assert.h>
#include <sstream>
#include <stddef.h>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "indexlib/indexlib.h"
#include "suez/turing/expression/util/AttributeInfos.h"
#include "suez/turing/expression/util/CustomizedTableInfo.h"
#include "suez/turing/expression/util/FieldInfos.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/SummaryInfo.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, TableInfo);

TableInfo::TableInfo(const std::string &tableName) : _tableName(tableName) {
    _fieldInfos = NULL;
    _indexInfos = NULL;
    _attributeInfos = NULL;
    _summaryInfo = NULL;
    _customizedTableInfo = NULL;
    _hasPrimaryKeyAttribute = false;
    _hasSubSchema = false;
}

TableInfo::~TableInfo() {
    DELETE_AND_SET_NULL(_fieldInfos);
    DELETE_AND_SET_NULL(_indexInfos);
    DELETE_AND_SET_NULL(_attributeInfos);
    DELETE_AND_SET_NULL(_summaryInfo);
    DELETE_AND_SET_NULL(_customizedTableInfo);
}

TableInfo::TableInfo(const TableInfo &tableInfo) {
    _fieldInfos = NULL;
    _indexInfos = NULL;
    _attributeInfos = NULL;
    _summaryInfo = NULL;
    _customizedTableInfo = NULL;

    _tableName = tableInfo._tableName;
    if (tableInfo._fieldInfos) {
        _fieldInfos = new FieldInfos(*(tableInfo._fieldInfos));
    }
    if (tableInfo._indexInfos) {
        _indexInfos = new IndexInfos(*(tableInfo._indexInfos));
    }
    if (tableInfo._attributeInfos) {
        _attributeInfos = new AttributeInfos(*(tableInfo._attributeInfos));
    }
    if (tableInfo._summaryInfo) {
        _summaryInfo = new SummaryInfo(*(tableInfo._summaryInfo));
    }
    if (tableInfo._customizedTableInfo) {
        _customizedTableInfo = new CustomizedTableInfo(*(tableInfo._customizedTableInfo));
    }
    _hasPrimaryKeyAttribute = tableInfo._hasPrimaryKeyAttribute;
    _hasSubSchema = tableInfo._hasSubSchema;
    _subTableName = tableInfo._subTableName;
}

void TableInfo::setTableName(const std::string &tableName) { _tableName = tableName; }

void TableInfo::setTableType(uint32_t tableType) { _tableType = tableType; }

void TableInfo::setTableVersion(int32_t tableVersion) { _tableVersion = tableVersion; }

void TableInfo::setFieldInfos(FieldInfos *fieldInfos) {
    if (fieldInfos) {
        if (_fieldInfos) {
            delete _fieldInfos;
        }
        _fieldInfos = fieldInfos;
    }
}

void TableInfo::setIndexInfos(IndexInfos *indexInfos) {
    if (indexInfos) {
        if (_indexInfos) {
            delete _indexInfos;
        }
        _indexInfos = indexInfos;
    }
}

void TableInfo::setAttributeInfos(AttributeInfos *attributeInfos) {
    assert(attributeInfos);
    if (attributeInfos) {
        if (_attributeInfos) {
            delete _attributeInfos;
        }
        _attributeInfos = attributeInfos;
    }
}

void TableInfo::setSummaryInfo(SummaryInfo *summaryInfo) {
    assert(summaryInfo);
    if (summaryInfo) {
        if (_summaryInfo) {
            delete _summaryInfo;
        }
        _summaryInfo = summaryInfo;
    }
}

void TableInfo::setCustomizedTableInfo(CustomizedTableInfo *customizedTableInfo) {
    if (customizedTableInfo) {
        if (_customizedTableInfo) {
            delete _customizedTableInfo;
        }
        _customizedTableInfo = customizedTableInfo;
    }
}

const IndexInfo *TableInfo::getPrimaryKeyIndexInfo() const {
    for (IndexInfos::ConstIterator it = _indexInfos->begin(); it != _indexInfos->end(); ++it) {
        IndexInfo *info = *it;
        if (!info->isSubDocIndex &&
            (info->getIndexType() == it_primarykey64 || info->getIndexType() == it_primarykey128 ||
             info->getIndexType() == it_kv || info->getIndexType() == it_kkv)) {
            return info;
        }
    }
    return NULL;
}

const IndexInfo *TableInfo::getSubDocPrimaryKeyIndexInfo() const {
    for (IndexInfos::ConstIterator it = _indexInfos->begin(); it != _indexInfos->end(); ++it) {
        IndexInfo *info = *it;
        if (info->isSubDocIndex &&
            (info->getIndexType() == it_primarykey64 || info->getIndexType() == it_primarykey128)) {
            return info;
        }
    }
    return NULL;
}

AUTIL_LOG_SETUP(config, TableMap);

const TableInfoPtr TableMap::getTableInfo(const std::string &tableName) const {
    map<string, TableInfoPtr>::const_iterator it;
    if ((it = find(tableName)) != end()) {
        return it->second;
    }
    AUTIL_LOG(TRACE2, "NOT found the TableInfo, name: %s", tableName.c_str());
    return TableInfoPtr();
}

void TableMap::addTableInfo(const TableInfoPtr &tableInfoPtr) {
    insert(value_type(tableInfoPtr->getTableName(), tableInfoPtr));
}

} // namespace turing
} // namespace suez
