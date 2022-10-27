#include <ha3/sql/rank/ComparatorCreator.h>
#include <ha3/sql/rank/OneDimColumnComparator.h>
#include <ha3/sql/rank/ColumnComparator.h>
#include <matchdoc/ValueType.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, ComparatorCreator);
ComboComparatorPtr ComparatorCreator::createOneDimColumnComparator(
        const TablePtr &table,
        const string &refName,
        const bool order)
{
    ColumnPtr column = table->getColumn(refName);
    if (column == nullptr) {
        SQL_LOG(ERROR, "invalid column name [%s]", refName.c_str());
        return ComboComparatorPtr();
    }
    switch (column->getColumnSchema()->getType().getBuiltinType()) {
#define CASE_MACRO(ft)                                                  \
        case ft: {                                                      \
            bool isMulti = column->getColumnSchema()->getType().isMultiValue(); \
            if (isMulti) {                                              \
                typedef MatchDocBuiltinType2CppType<ft, true>::CppType T; \
                ColumnData<T> *columnData = column->getColumnData<T>(); \
                if (unlikely(!columnData)) {                            \
                    SQL_LOG(ERROR, "impossible cast column data failed"); \
                    return ComboComparatorPtr();                        \
                }                                                       \
                if (order) {                                            \
                    return ComboComparatorPtr(new                       \
                            OneDimColumnDescComparator<T>(columnData)); \
                } else {                                                \
                    return ComboComparatorPtr(new                       \
                            OneDimColumnAscComparator<T>(columnData));  \
                }                                                       \
            } else {                                                    \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                ColumnData<T> *columnData = column->getColumnData<T>(); \
                if (unlikely(!columnData)) {                            \
                    SQL_LOG(ERROR, "impossible cast column data failed"); \
                    return ComboComparatorPtr();                        \
                }                                                       \
                if (order) {                                            \
                    return ComboComparatorPtr(new                       \
                            OneDimColumnDescComparator<T>(columnData)); \
                } else {                                                \
                    return ComboComparatorPtr(new                       \
                            OneDimColumnAscComparator<T>(columnData));  \
                }                                                       \
            }                                                           \
        }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        SQL_LOG(ERROR, "impossible reach this branch");
        return ComboComparatorPtr();
    }
    }//switch
    return ComboComparatorPtr();
}

ComboComparatorPtr ComparatorCreator::createComboComparator(
        const TablePtr &table,
        const vector<string> &refNames,
        const vector<bool> &orders,
        autil::mem_pool::Pool *pool)
{
    ComboComparatorPtr comboComparator(new ComboComparator());
    for (size_t i = 0; i < refNames.size(); i++) {
        ColumnPtr column = table->getColumn(refNames[i]);
        if (column == nullptr) {
            SQL_LOG(ERROR, "invalid column name [%s]", refNames[i].c_str());
            return ComboComparatorPtr();
        }
        switch (column->getColumnSchema()->getType().getBuiltinType()) {
#define CASE_MACRO(ft)                                                  \
            case ft: {                                                  \
                bool isMulti = column->getColumnSchema()->getType().isMultiValue(); \
                if (isMulti) {                                          \
                    typedef MatchDocBuiltinType2CppType<ft, true>::CppType T; \
                    ColumnData<T> *columnData = column->getColumnData<T>(); \
                    if (unlikely(!columnData)) {                        \
                        SQL_LOG(ERROR, "impossible cast column data failed"); \
                        return ComboComparatorPtr();                    \
                    }                                                   \
                    if (orders[i]) {                                    \
                        auto comparator = POOL_NEW_CLASS(pool,          \
                                ColumnDescComparator<T>,                \
                                columnData);                            \
                        comboComparator->addComparator(comparator);     \
                    } else {                                            \
                        auto comparator = POOL_NEW_CLASS(pool,          \
                                ColumnAscComparator<T>,                 \
                                columnData);                            \
                        comboComparator->addComparator(comparator);     \
                    }                                                   \
                } else {                                                \
                    typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                    ColumnData<T> *columnData = column->getColumnData<T>(); \
                    if (unlikely(!columnData)) {                        \
                        SQL_LOG(ERROR, "impossible cast column data failed"); \
                        return ComboComparatorPtr();                    \
                    }                                                   \
                    if (orders[i]) {                                    \
                        auto comparator = POOL_NEW_CLASS(pool,          \
                                ColumnDescComparator<T>,                \
                                columnData);                            \
                        comboComparator->addComparator(comparator);     \
                    } else {                                            \
                        auto comparator = POOL_NEW_CLASS(pool,          \
                                ColumnAscComparator<T>,                 \
                                columnData);                            \
                        comboComparator->addComparator(comparator);     \
                    }                                                   \
                }                                                       \
                break;                                                  \
            }
            BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        default: {
            SQL_LOG(ERROR, "impossible reach this branch");
            return ComboComparatorPtr();
        }
        }//switch
    }
    return comboComparator;
}

END_HA3_NAMESPACE(sql);
