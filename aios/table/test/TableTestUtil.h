#pragma once
#include "gtest/gtest.h"
#include <string>
#include <vector>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "table/ColumnComparator.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/test/MatchDocUtil.h"

namespace table {

class TableTestUtil {
public:
    template <typename T>
    static void checkValueEQ(const T &expected, const T &actual, const std::string &msg = "") {
        ASSERT_EQ(expected, actual) << msg;
    }
    static void checkValueEQ(const double &expected, const double &actual, const std::string &msg = "") {
        ASSERT_DOUBLE_EQ(expected, actual) << msg;
    }
    static void checkValueEQ(const std::string &expected, const autil::MultiChar &actual, const std::string &msg = "") {
        ASSERT_EQ(expected, std::string(actual.data(), actual.size())) << msg;
    }

    template <typename T>
    static void checkOutputColumn(const TablePtr &table, const std::string &columnName, const std::vector<T> &expects) {
        ASSERT_NE(nullptr, table);
        typedef typename ColumnDataTypeTraits<T>::value_type ColumnDataType;
        auto column = table->getColumn(columnName);
        ASSERT_NE(nullptr, column) << "columnName: " << columnName;
        auto result = column->getColumnData<ColumnDataType>();
        ASSERT_NE(nullptr, result) << "columnType not match, columnName: " << columnName;

        std::ostringstream oss;
        oss << "expected: " << autil::StringUtil::toString(expects, ",") << std::endl;
        oss << "actual: ";
        for (size_t i = 0; i < table->getRowCount(); ++i) {
            oss << result->toString(i) << ",";
        }
        oss << std::endl;
        std::string msg = oss.str();

        ASSERT_EQ(expects.size(), table->getRowCount()) << msg;
        for (size_t i = 0; i < expects.size(); i++) {
            ASSERT_NO_FATAL_FAILURE(checkValueEQ(expects[i], result->get(i), msg));
        }
    }
    // for template parameter deduce
    static void
    checkOutputColumn(const TablePtr &table, const std::string &columnName, const std::vector<std::string> &expects) {
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<std::string>(table, columnName, expects));
    }

    template <typename T>
    static void checkOutputMultiColumn(const TablePtr &table,
                                       const std::string &columnName,
                                       const std::vector<std::vector<T>> &expects) {
        typedef typename ColumnDataTypeTraits<T>::value_type ColumnDataType;
        ASSERT_EQ(expects.size(), table->getRowCount());
        auto column = table->getColumn(columnName);
        ASSERT_TRUE(column != nullptr);
        auto result = column->getColumnData<autil::MultiValueType<ColumnDataType>>();
        ASSERT_TRUE(result != nullptr);

        autil::mem_pool::Pool pool;
        for (size_t i = 0; i < expects.size(); i++) {
            autil::MultiValueType<ColumnDataType> p(
                autil::MultiValueCreator::createMultiValueBuffer(expects[i], &pool));
            ASSERT_EQ(p, result->get(i)) << p << "|" << result->get(i);
        }
    }
    // for template parameter deduce
    static void checkOutputMultiColumn(const TablePtr &table,
                                       const std::string &columnName,
                                       const std::vector<std::vector<std::string>> &expects) {
        ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<std::string>(table, columnName, expects));
    }

    template <typename T>
    static void checkMultiValue(const std::vector<T> &expects, const autil::MultiValueType<T> &actual) {
        autil::mem_pool::Pool pool;
        autil::MultiValueType<T> p(autil::MultiValueCreator::createMultiValueBuffer(expects, &pool));
        ASSERT_EQ(p, actual);
    }

    static TablePtr createTable(matchdoc::MatchDocAllocatorPtr allocator, std::vector<matchdoc::MatchDoc> docs) {
        return Table::fromMatchDocs(docs, allocator);
    }

    static void
    checkTraceCount(size_t expectedNums, const std::string &filter, const std::vector<std::string> &traces) {
        size_t count = std::count_if(
            traces.begin(), traces.end(), [&](auto &text) { return text.find(filter) != std::string::npos; });
        ASSERT_EQ(expectedNums, count) << "filter: " << filter;
    }

    template <typename T>
    static void checkVector(const std::vector<T> &expects, const std::vector<T> &actuals) {
        ASSERT_EQ(expects.size(), actuals.size());
        for (size_t i = 0; i < expects.size(); i++) {
            ASSERT_EQ(expects[i], actuals[i]);
        }
    }

    template <typename T>
    static void sortByColumn(TablePtr &table, const std::string &colName, bool asc = true) {
        auto *column = table->getColumn(colName);
        ASSERT_NE(nullptr, column);
        auto *columnData = column->getColumnData<T>();
        ASSERT_NE(nullptr, columnData);
        ColumnAscComparator<T> comparator(columnData);
        TableUtil::sort(table, &comparator);
    }

    static void checkHllCtxOutputColumn(TablePtr &table, const std::string &colName, autil::HllCtx *expectCtx) {
        ASSERT_EQ(1, table->getRowCount());
        auto *column = table->getColumn(colName);
        ASSERT_NE(nullptr, column);
        auto *columnData = column->getColumnData<autil::HllCtx>();
        ASSERT_NE(nullptr, columnData);
        autil::HllCtx value = columnData->get(0);
        ASSERT_EQ(autil::Hyperloglog::hllCtxCount(expectCtx), autil::Hyperloglog::hllCtxCount(&value));
    }
};
} // namespace table
