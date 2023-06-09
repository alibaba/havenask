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

#include <assert.h>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"

#include "table/Row.h"

namespace table {

class ColumnDataBase {
public:
    ColumnDataBase(matchdoc::ReferenceBase* ref, std::vector<Row>* rows)
        : _ref(ref)
        , _rows(rows)
    {}
    virtual ~ColumnDataBase() {}
public:
    std::string toString(size_t rowIndex) const;
    std::string toOriginString(size_t rowIndex) const;
    inline size_t getRowCount() const {return _rows->size();}
private:
    matchdoc::ReferenceBase* _ref;
protected:
    std::vector<Row>* _rows;
};

template <typename T>
class ColumnData : public ColumnDataBase
{
public:
    ColumnData(matchdoc::Reference<T>* ref, std::vector<Row>* rows)
        : ColumnDataBase(ref, rows)
        , _ref(ref)
    {}
    ~ColumnData() {}
public:
    inline T get(size_t rowIndex) const;
    inline T get(const Row &row) const;

    inline T* getPointer(size_t rowIndex) const;
    inline T* getPointer(const Row &row) const;

    inline void set(size_t rowIndex, const T& value);
    inline void set(const Row &row, const T& value);
private:
    matchdoc::Reference<T>* _ref;
};

template<typename T>
inline T ColumnData<T>::get(size_t rowIndex) const {
    assert(rowIndex < _rows->size());
    return _ref->get((*_rows)[rowIndex]);
}

template<typename T>
inline T ColumnData<T>::get(const Row &row) const {
    return _ref->get(row);
}

template<typename T>
inline  T* ColumnData<T>::getPointer(size_t rowIndex) const {
    assert(rowIndex < _rows->size());
    return _ref->getPointer((*_rows)[rowIndex]);
}

template<typename T>
inline T* ColumnData<T>::getPointer(const Row &row) const {
    return _ref->getPointer(row);
}

template<typename T>
inline void ColumnData<T>::set(size_t rowIndex, const T& value) {
    assert(rowIndex < _rows->size());
    _ref->set((*_rows)[rowIndex], value);
}

template<typename T>
inline void ColumnData<T>::set(const Row &row, const T& value) {
    _ref->set(row, value);
}

}
