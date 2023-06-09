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
#include "indexlib/analyzer/TraditionalTables.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace indexlibv2 { namespace analyzer {
AUTIL_LOG_SETUP(indexlib.analyzer, TraditionalTables);

static const std::string TRADITIONAL_TABLES = "traditional_tables";
static const std::string TRADITIONAL_TABLE_NAME = "table_name";
static const std::string TRANSFORM_TABLE = "transform_table";

void TraditionalTable::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize(TRADITIONAL_TABLE_NAME, tableName, string());
    map<string, string> strTable;
    json.Jsonize(TRANSFORM_TABLE, strTable, strTable);
    for (map<string, string>::const_iterator it = strTable.begin(); it != strTable.end(); ++it) {
        uint16_t key;
        if (!hexToUint16(it->first, key)) {
            throw autil::legacy::ExceptionBase("Invalid hex code: [" + it->first + "]");
        }
        uint16_t value;
        if (!hexToUint16(it->second, value)) {
            throw autil::legacy::ExceptionBase("Invalid hex code: [" + it->second + "]");
        }
        table[key] = value;
    }
}

bool TraditionalTable::hexToUint16(const string& str, uint16_t& value)
{
    uint64_t value64;
    if (!StringUtil::hexStrToUint64(str.c_str(), value64)) {
        return false;
    }
    if (value64 > numeric_limits<uint16_t>::max()) {
        return false;
    }
    value = (uint16_t)value64;
    return true;
}

////////////////////////////////////////////////////////////////////////////////
void TraditionalTables::Jsonize(Jsonizable::JsonWrapper& json) { json.Jsonize(TRADITIONAL_TABLES, _tables, _tables); }

const TraditionalTable* TraditionalTables::getTraditionalTable(const std::string& tableName) const
{
    for (vector<TraditionalTable>::const_iterator it = _tables.begin(); it != _tables.end(); ++it) {
        if (it->tableName == tableName) {
            return &(*it);
        }
    }
    return NULL;
}

void TraditionalTables::addTraditionalTable(const TraditionalTable& table) { _tables.push_back(table); }

}} // namespace indexlibv2::analyzer
