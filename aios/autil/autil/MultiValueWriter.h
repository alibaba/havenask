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

#include "autil/MultiValueFormatter.h"
#include "autil/MultiValueType.h"

namespace autil {

template <typename T>
class MultiValueWriter
{
public:
    MultiValueWriter()
        : _data(NULL)
        , _recordNum(0)
    {}
    
    ~MultiValueWriter() {}
private:
    MultiValueWriter(const MultiValueWriter &);
    MultiValueWriter& operator = (const MultiValueWriter &);
public:
    bool init(char* buffer, size_t bufLen, size_t recordNum);
    bool set(size_t idx, const T& value);
    T* getData() { return _data; }
private:
    T* _data;
    size_t _recordNum;
};

template <typename T>
inline bool MultiValueWriter<T>::init(char* buffer, size_t bufLen, size_t recordNum)
{
    if (!buffer) {
        return false;
    }
    size_t countLen = MultiValueFormatter::encodeCount(recordNum, buffer, bufLen);
    if (countLen == 0) {
        return false;
    }

    if (bufLen - countLen < recordNum * sizeof(T)) {
        return false;
    }
    _data = (T*)(buffer + countLen);
    _recordNum = recordNum;
    return true;
}

template <>
inline bool MultiValueWriter<MultiChar>::init(char* buffer, size_t bufLen, size_t recordNum)
{
    assert(false);
    return false;
}

template <typename T>
inline bool MultiValueWriter<T>::set(size_t idx, const T& value) {
    if (idx >= _recordNum) {
        return false;
    }
    _data[idx] = value;
    return true;
}

template <>
inline bool MultiValueWriter<MultiChar>::set(size_t idx, const MultiChar& value) {
    assert(false);
    return false;
}

}

