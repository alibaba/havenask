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

#include "table/Comparator.h"
#include "table/Row.h"

namespace table {
template <typename T>
class ColumnData;
} // namespace table

namespace table {

namespace __detail {
template <typename T, bool asc>
class ColumnComparator : public Comparator {
public:
    ColumnComparator(const ColumnData<T> *columnData) : _columnData(columnData) {}
    ~ColumnComparator() {}

private:
    ColumnComparator(const ColumnComparator &);
    ColumnComparator &operator=(const ColumnComparator &);

public:
    bool compare(Row a, Row b) const override {
        if constexpr (asc) {
            return _columnData->get(a) < _columnData->get(b);
        } else {
            return _columnData->get(b) < _columnData->get(a);
        }
    }

private:
    const ColumnData<T> *_columnData;
};
} // namespace __detail

template <typename T>
using ColumnAscComparator = __detail::ColumnComparator<T, true>;

template <typename T>
using ColumnDescComparator = __detail::ColumnComparator<T, false>;

} // namespace table
