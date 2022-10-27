#include "build_service/analyzer/TraditionalTables.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace build_service {
namespace analyzer {
BS_LOG_SETUP(analyzer, TraditionalTables);

static const std::string TRADITIONAL_TABLES = "traditional_tables";
static const std::string TRADITIONAL_TABLE_NAME = "table_name";
static const std::string TRANSFORM_TABLE = "transform_table";

void TraditionalTable::Jsonize(Jsonizable::JsonWrapper& json) {
    json.Jsonize(TRADITIONAL_TABLE_NAME, tableName, "");
    map<string, string> strTable;
    json.Jsonize(TRANSFORM_TABLE, strTable, strTable);
    for (map<string, string>::const_iterator it = strTable.begin();
         it != strTable.end(); ++it)
    {
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

bool TraditionalTable::hexToUint16(const string &str, uint16_t &value) {
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

TraditionalTables::TraditionalTables() {
}

TraditionalTables::~TraditionalTables() {
}

void TraditionalTables::Jsonize(Jsonizable::JsonWrapper& json) {
    json.Jsonize(TRADITIONAL_TABLES, _tables, _tables);
}

const TraditionalTable *TraditionalTables::getTraditionalTable(const std::string &tableName) const {
    for (vector<TraditionalTable>::const_iterator it = _tables.begin();
         it != _tables.end(); ++it)
    {
        if (it->tableName == tableName) {
            return &(*it);
        }
    }
    return NULL;
}

void TraditionalTables::addTraditionalTable(const TraditionalTable &table) {
    _tables.push_back(table);
}

}
}
