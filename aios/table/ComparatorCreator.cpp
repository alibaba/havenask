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
#include "table/ComparatorCreator.h"

#include <memory>

#include "autil/CommonMacros.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "matchdoc/ValueType.h"
#include "table/Column.h"
#include "table/ColumnComparator.h"
#include "table/ColumnSchema.h"
#include "table/ComboComparator.h"
#include "table/Table.h"

namespace table {
template <typename T>
class ColumnData;
} // namespace table

using namespace std;
using namespace matchdoc;

namespace table {
AUTIL_LOG_SETUP(sql, ComparatorCreator);

ComboComparatorPtr ComparatorCreator::createComboComparator(const TablePtr &table,
                                                            const vector<string> &refNames,
                                                            const vector<bool> &orders,
                                                            autil::mem_pool::Pool *pool) {
    ComboComparatorPtr comboComparator(new ComboComparator());
    for (size_t i = 0; i < refNames.size(); i++) {
        auto column = table->getColumn(refNames[i]);
        if (column == nullptr) {
            AUTIL_LOG(ERROR, "invalid column name [%s]", refNames[i].c_str());
            return ComboComparatorPtr();
        }
        switch (column->getColumnSchema()->getType().getBuiltinType()) {
#define CASE_MACRO(ft)                                                                                                 \
    case ft: {                                                                                                         \
        bool isMulti = column->getColumnSchema()->getType().isMultiValue();                                            \
        if (isMulti) {                                                                                                 \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                                                  \
            ColumnData<T> *columnData = column->getColumnData<T>();                                                    \
            if (unlikely(!columnData)) {                                                                               \
                AUTIL_LOG(ERROR, "impossible cast column data failed");                                                \
                return ComboComparatorPtr();                                                                           \
            }                                                                                                          \
            if (orders[i]) {                                                                                           \
                auto comparator = POOL_NEW_CLASS(pool, ColumnDescComparator<T>, columnData);                           \
                comboComparator->addComparator(comparator);                                                            \
            } else {                                                                                                   \
                auto comparator = POOL_NEW_CLASS(pool, ColumnAscComparator<T>, columnData);                            \
                comboComparator->addComparator(comparator);                                                            \
            }                                                                                                          \
        } else {                                                                                                       \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                                 \
            ColumnData<T> *columnData = column->getColumnData<T>();                                                    \
            if (unlikely(!columnData)) {                                                                               \
                AUTIL_LOG(ERROR, "impossible cast column data failed");                                                \
                return ComboComparatorPtr();                                                                           \
            }                                                                                                          \
            if (orders[i]) {                                                                                           \
                auto comparator = POOL_NEW_CLASS(pool, ColumnDescComparator<T>, columnData);                           \
                comboComparator->addComparator(comparator);                                                            \
            } else {                                                                                                   \
                auto comparator = POOL_NEW_CLASS(pool, ColumnAscComparator<T>, columnData);                            \
                comboComparator->addComparator(comparator);                                                            \
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }
            BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        default: {
            AUTIL_LOG(ERROR, "impossible reach this branch");
            return ComboComparatorPtr();
        }
        } // switch
    }
    return comboComparator;
}

} // namespace table
