#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/data/TableJson.h>
#include <autil/legacy/jsonizable.h>
#include <autil/TimeUtility.h>
#include <matchdoc/ValueType.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace autil::legacy;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, TableUtil);

TableUtil::TableUtil() {
}

TableUtil::~TableUtil() {
}

void TableUtil::sort(const TablePtr &table, ComboComparator *comparator) {
    assert(table != nullptr);
    assert(comparator);
    vector<Row> rows = table->getRows();
    std::sort(rows.begin(), rows.end(),
              [&comparator] (Row a, Row b) { return comparator->compare(a, b); });
    table->setRows(rows);
}

void TableUtil::topK(const TablePtr &table, ComboComparator *comparator, size_t topk) {
    assert(table != nullptr);
    assert(comparator);
    size_t rowCount = table->getRowCount();
    if (topk >= rowCount) {
        return;
    }
    vector<Row> rows = table->getRows();
    nth_element(rows.begin(), rows.begin() + topk, rows.end(),
                [&comparator] (Row a, Row b) { return comparator->compare(a, b); });
    rows.resize(topk);
    table->setRows(rows);
}

void TableUtil::topK(const TablePtr &table, ComboComparator *comparator, size_t topk, vector<Row> &rowVec) {
    assert(table != nullptr);
    assert(comparator);
    if (topk >= rowVec.size()) {
        return;
    }
    nth_element(rowVec.begin(), rowVec.begin() + topk, rowVec.end(),
                [&comparator] (Row a, Row b) { return comparator->compare(a, b); });
    rowVec.resize(topk);
}

string TableUtil::toString(const TablePtr &table) {
    if (table == nullptr) {
        return "";
    }
    ostringstream oss;
    size_t rowCount = table->getRowCount();
    size_t colCount = table->getColumnCount();
    oss.flags(ios::right);
    for (size_t col = 0; col < colCount; col++) {
        oss << setw(30) << table->getColumnName(col)
            + "(" + valueTypeToString(table->getColumnType(col)) + ")" + " |";
    }
    oss << "\n";

    for (size_t row = 0; row < rowCount; row++) {
        for (size_t col = 0; col < colCount; col++) {
            oss << setw(30) << table->toString(row, col) + " |";
        }
        oss << "\n";
    }
    return oss.str();
}

string TableUtil::toString(const TablePtr &table, size_t maxRowCount) {
    if (table == nullptr) {
        return "";
    }
    ostringstream oss;
    size_t rowCount = table->getRowCount();
    size_t colCount = table->getColumnCount();
    oss <<"row count:" << rowCount <<", col count:" <<colCount<<endl;
    oss.flags(ios::right);
    for (size_t col = 0; col < colCount; col++) {
        oss << setw(30) << table->getColumnName(col)
            + " (" + valueTypeToString(table->getColumnType(col)) + ")" + " |";
    }
    oss << "\n";

    for (size_t row = 0; row < rowCount && row < maxRowCount; row++) {
        for (size_t col = 0; col < colCount; col++) {
            oss << setw(30) << table->toString(row, col) + " |";
        }
        oss << "\n";
    }
    return oss.str();
}

string TableUtil::rowToString(const TablePtr &table, size_t rowOffset) {
    if (table == nullptr || rowOffset >= table->getRowCount()) {
        return "";
    }
    ostringstream oss;
    size_t rowCount = table->getRowCount();
    size_t colCount = table->getColumnCount();
    oss <<"row count:" << rowCount <<", col count:" <<colCount<<endl;
    for (size_t col = 0; col < colCount; col++) {
        oss << setw(25) << table->getColumnName(col) << " |";
    }
    oss << "\n";
    for (size_t col = 0; col < colCount; col++) {
        oss << setw(25) << table->toString(rowOffset, col) << " |";
    }
    oss << "\n";
    return oss.str();
}

std::string TableUtil::valueTypeToString(ValueType vt) {
    if (!vt.isBuiltInType()) return "unknown_type";
    std::string ret = "";
    if (vt.isMultiValue()) {
        ret = "multi_";
    }
    switch (vt.getBuiltinType()) {
    case matchdoc::bt_bool   : { ret += "bool"; break; }
    case matchdoc::bt_int8   : { ret += "int8"; break; }
    case matchdoc::bt_int16  : { ret += "int16"; break; }
    case matchdoc::bt_int32  : { ret += "int32"; break; }
    case matchdoc::bt_int64  : { ret += "int64"; break; }
    case matchdoc::bt_uint8  : { ret += "uint8"; break; }
    case matchdoc::bt_uint16 : { ret += "uint16"; break; }
    case matchdoc::bt_uint32 : { ret += "uint32"; break; }
    case matchdoc::bt_uint64 : { ret += "uint64"; break; }
    case matchdoc::bt_float  : { ret += "float"; break; }
    case matchdoc::bt_double : { ret += "double"; break; }
    case matchdoc::bt_string : { ret += "multi_char"; break; }
    default : ret = "unknown_type";
    }
    return ret;
}

bool TableUtil::toTableJson(TablePtr &table, TableJson &tableJson) {
    assert(table != nullptr);
    size_t rowCount = table->getRowCount();
    size_t colCount = table->getColumnCount();
    tableJson.data.resize(rowCount, vector<Any>(colCount));
    tableJson.columnName.resize(colCount);
    tableJson.columnType.resize(colCount);
    for (size_t col = 0; col < colCount; col++) {
        auto column = table->getColumn(col);
        if (!column) {
            SQL_LOG(ERROR, "get column [%lu] failed", col);
            return false;
        }
        auto vt = column->getColumnSchema()->getType();
        bool isMulti = vt.isMultiValue();
        switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                  \
            case ft: {                                                  \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                if (isMulti) {                                          \
                    auto columnData = column->getColumnData<autil::MultiValueType<T>>(); \
                    if (columnData == nullptr) {                        \
                        SQL_LOG(ERROR, "get column data [%lu] failed", col); \
                        return false;                                      \
                    }                                                   \
                    vector<T> valVec;                                   \
                    for (size_t row = 0; row < rowCount; row++) {       \
                        valVec.clear();                                 \
                        auto val = columnData->get(row);                \
                        for (size_t i = 0; i < val.size(); ++i) {       \
                            valVec.emplace_back(val[i]);                \
                        }                                               \
                        tableJson.data[row][col] = ToJson(valVec);      \
                    }                                                   \
                } else {                                                \
                    auto columnData = column->getColumnData<T>();       \
                    if (columnData == nullptr) {                        \
                        SQL_LOG(ERROR, "get column data [%lu] failed", col); \
                        return false;                                      \
                    }                                                   \
                    for (size_t row = 0; row < rowCount; row++) {       \
                        T val = columnData->get(row);                   \
                        tableJson.data[row][col] = ToJson(val);         \
                    }                                                   \
                }                                                       \
                break;                                                  \
            }
            NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        case bt_string: {
            if (isMulti) {
                auto columnData = column->getColumnData<autil::MultiValueType<autil::MultiChar>>();
                if (columnData == nullptr) {
                    SQL_LOG(ERROR, "get column data [%lu] failed", col);
                    return false;
                }
                vector<string> valVec;
                for (size_t row = 0; row < rowCount; row++) {
                    valVec.clear();
                    auto val = columnData->get(row);
                    for (size_t i = 0; i < val.size(); ++i) {
                        valVec.emplace_back(string(val[i].data(), val[i].size()));
                    }
                    tableJson.data[row][col] = ToJson(valVec);
                }
            } else {
                auto columnData = column->getColumnData<autil::MultiChar>();
                if (columnData == nullptr) {
                    SQL_LOG(ERROR, "get column data [%lu] failed", col);
                    return false;
                }
                for (size_t row = 0; row < rowCount; row++) {
                    const autil::MultiChar &val = columnData->get(row);
                    tableJson.data[row][col] = ToJson(string(val.data(), val.size()));
                }
            }
            break;

        }
        case bt_bool: {
            if (isMulti) {
                SQL_LOG(ERROR, "multi bool type not supported");
                return false;
            } else {
                auto columnData = column->getColumnData<bool>();
                if (columnData == nullptr) {
                    SQL_LOG(ERROR, "get column data [%lu] failed", col);
                    return false;
                }
                for (size_t row = 0; row < rowCount; row++) {
                    bool val = columnData->get(row);
                    tableJson.data[row][col] = ToJson(val);
                }
            }
            break;
        }
        default: {
            SQL_LOG(ERROR, "impossible reach this branch");
            return false;
        }
        } // switch
    }

    for (size_t col = 0; col < colCount; col++) {
        auto column = table->getColumn(col);
        if (column == nullptr) {
            return false;
        }
        auto columnSchema = column->getColumnSchema();
        tableJson.columnName[col] = columnSchema->getName();
        tableJson.columnType[col] = valueTypeToString(columnSchema->getType());
    }
    table.reset();
    return true;
}

string TableUtil::toJsonString(TablePtr &table) {
    TableJson tableJson;
    if (!TableUtil::toTableJson(table, tableJson)) {
        SQL_LOG(ERROR, "to table json faild.");
        return "";
    }
    try {
        return FastToJsonString(tableJson, true);
    } catch (const ExceptionBase &e) {
        SQL_LOG(ERROR, "format table json failed. exception:[%s]",  e.what());
        return "";
    }
}

bool TableUtil::reorderColumn(const TablePtr &table,
                              const std::vector<std::string> &columnNames)
{
    assert(table != nullptr);
    if (columnNames.size() != table->getColumnCount()) {
        SQL_LOG(WARN, "column count mismatch, columns size [%lu], table column count [%lu]",
                columnNames.size(), table->getColumnCount());
        return false;
    }
    std::vector<ColumnPtr> columnVec;
    for (auto it = columnNames.begin(); it != columnNames.end(); it++) {
        if (find(it + 1, columnNames.end(), *it) != columnNames.end()) {
            SQL_LOG(WARN, "duplicate column name. column names %s",
                    autil::StringUtil::toString(columnNames).c_str());
            return false;
        }
        auto column = table->getColumn(*it);
        if (column == nullptr) {
            SQL_LOG(WARN, "column [%s] not exist", it->c_str());
            return false;
        }
        columnVec.emplace_back(column);
    }
    table->setColumnVec(columnVec);
    return true;
}

END_HA3_NAMESPACE(sql);
