#ifndef ISEARCH_TABLEUTIL_H
#define ISEARCH_TABLEUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/data/TableJson.h>
#include <ha3/sql/rank/ComboComparator.h>
#include <ha3/util/cityhash/city.h>


BEGIN_HA3_NAMESPACE(sql);

class TableUtil
{
public:
    TableUtil();
    ~TableUtil();
private:
    TableUtil(const TableUtil &);
    TableUtil& operator=(const TableUtil &);
public:
    static void sort(const TablePtr &table, ComboComparator *comparator);
    static void topK(const TablePtr &table, ComboComparator *comparator, size_t topk);
    static void topK(const TablePtr &table, ComboComparator *comparator,
                     size_t topk, std::vector<Row> &rowVec);
    template <typename T>
    static size_t calculateHashValue(const T &data);
    static std::string toString(const TablePtr &table);
    static std::string toString(const TablePtr &table, size_t rowCount);
    static std::string rowToString(const TablePtr &table, size_t rowOffset);
    static std::string toJsonString(TablePtr &table);
    static bool toTableJson(TablePtr &table, TableJson &tableJson);
    template<typename T>
    static ColumnData<T> *getColumnData(const TablePtr &table, const std::string &name);
    template<typename T>
    static ColumnData<T> *tryGetTypedColumnData(const TablePtr &table,
            const std::string &name);
    template<typename T>
    static ColumnData<T> *declareAndGetColumnData(const TablePtr &table, const std::string &name,
            bool endGroup = false, bool existAsError = true);
    static std::string valueTypeToString(ValueType vt);
    static bool reorderColumn(const TablePtr &table, const std::vector<std::string> &columnNames);

    template <class T>
    static void hash_combine(size_t& seed, const T& v) {
        std::hash<T> hasher;
        seed ^= hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
    }
private:
    HA3_LOG_DECLARE();

};

template<typename T>
size_t TableUtil::calculateHashValue(const T &data) {
    // ignore multi value
    return 0;
}
template<>
inline size_t TableUtil::calculateHashValue(const autil::MultiChar &data) {
    return isearch::util::CityHash64(data.data(), data.size());
}

template<>
inline size_t TableUtil::calculateHashValue(const std::string &data) {
    return isearch::util::CityHash64(data.data(), data.size());
}

#define CALCULATE_HASH_VALUE(Type)                                      \
    template<>                                                          \
    inline size_t TableUtil::calculateHashValue<Type>(const Type &data) { \
        std::hash<Type> hasher;                                         \
        return hasher(data);                                            \
    }

CALCULATE_HASH_VALUE(int8_t)
CALCULATE_HASH_VALUE(int16_t)
CALCULATE_HASH_VALUE(int32_t)
CALCULATE_HASH_VALUE(int64_t)
CALCULATE_HASH_VALUE(uint8_t)
CALCULATE_HASH_VALUE(uint16_t)
CALCULATE_HASH_VALUE(uint32_t)
CALCULATE_HASH_VALUE(uint64_t)
CALCULATE_HASH_VALUE(float)
CALCULATE_HASH_VALUE(double)

#undef CALCULATE_HASH_VALUE

#define CALCULATE_MULTI_VALUE_HASH_VALUE(Type)                          \
    template<>                                                          \
    inline size_t TableUtil::calculateHashValue<Type>(const Type &data) { \
        return isearch::util::CityHash64(data.getBaseAddress(), data.length()); \
    }
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiUInt8);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiInt8);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiUInt16);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiInt16);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiUInt32);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiInt32);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiUInt64);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiInt64);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiFloat);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiDouble);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiString);

#undef CALCULATE_MULTI_VALUE_HASH_VALUE


template<typename T>
ColumnData<T> *TableUtil::getColumnData(const TablePtr &table, const std::string &name) {
    assert(table != nullptr);
    auto column = table->getColumn(name);
    if (column == nullptr) {
        SQL_LOG(WARN, "column [%s] does not exist", name.c_str());
        return nullptr;
    }
    ColumnData<T> *ret = column->getColumnData<T>();
    if (ret == nullptr) {
        SQL_LOG(WARN, "column data [%s] type mismatch", name.c_str());
        return nullptr;
    }
    return ret;
}

template<typename T>
ColumnData<T> *TableUtil::tryGetTypedColumnData(const TablePtr &table,
        const std::string &name) {
    assert(table != nullptr);
    auto column = table->getColumn(name);
    if (column == nullptr) {
        return nullptr;
    }
    return column->getColumnData<T>();
}

template<typename T>
ColumnData<T> *TableUtil::declareAndGetColumnData(const TablePtr &table,
        const std::string &name, bool endGroup, bool existAsError)
{
    assert(table != nullptr);
    if (table->getColumn(name) != nullptr) {
        if (existAsError) {
            SQL_LOG(WARN, "duplicate column name [%s]", name.c_str());
            return nullptr;
        } else {
            return getColumnData<T>(table, name);
        }
    }
    auto column = table->declareColumn<T>(name, endGroup);
    if (column == nullptr) {
        SQL_LOG(WARN, "declare column [%s] failed", name.c_str());
        return nullptr;
    }
    return column->template getColumnData<T>();
}

template <>
inline void TableUtil::hash_combine(size_t& seed, const size_t& v) {
    seed ^= v + 0x9e3779b9 + (seed<<6) + (seed>>2);
}

HA3_TYPEDEF_PTR(TableUtil);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_TABLEUTIL_H
