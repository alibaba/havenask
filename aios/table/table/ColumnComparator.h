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

#include "table/Row.h"
#include "table/Comparator.h"

namespace table {
template <typename T> class ColumnData;
}  // namespace table

namespace table {

template <typename T>
class ColumnAscComparator : public Comparator
{
public:
    ColumnAscComparator(const ColumnData<T> *columnData)
        : _columnData(columnData)
    {}
    ~ColumnAscComparator() {}
private:
    ColumnAscComparator(const ColumnAscComparator &);
    ColumnAscComparator& operator=(const ColumnAscComparator &);
public:
    bool compare(Row a, Row b) const override {
        return _columnData->get(a) < _columnData->get(b);
    }
private:
    const ColumnData<T> *_columnData;
};


template <typename T>
class ColumnDescComparator : public Comparator
{
public:
    ColumnDescComparator(const ColumnData<T> *columnData)
        : _columnData(columnData)
    {}
    ~ColumnDescComparator() {}
private:
    ColumnDescComparator(const ColumnDescComparator &);
    ColumnDescComparator& operator=(const ColumnDescComparator &);
public:
    bool compare(Row a, Row b) const override {
        return _columnData->get(b) < _columnData->get(a);
    }
private:
    const ColumnData<T> *_columnData;
};

}

