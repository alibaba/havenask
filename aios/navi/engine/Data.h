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
#ifndef NAVI_DATA_H
#define NAVI_DATA_H

#include "navi/common.h"
#include "navi/engine/TypeInfo.h"

namespace navi {

class Type;

class Data
{
public:
    Data(const std::string &typeId);
    virtual ~Data();
public:
    // do not use this invalid ctor
    Data(const char *) = delete;
private:
    Data(const Data &);
    Data& operator=(const Data &);
public:
    const std::string &getTypeId() const {
        return _typeId;
    }
    virtual void show() const {
    }
private:
    const std::string &_typeId;
};

NAVI_TYPEDEF_PTR(Data);

struct StreamData {
public:
    StreamData() {
        reset();
    }
    void reset(bool clearEof = true) {
        hasValue = false;
        data.reset();
        if (clearEof) {
            eof = false;
        }
    }
public:
    bool hasValue;
    bool eof;
    DataPtr data;
};

#define BIND_DATA_TO(member)                                                   \
    [](void *obj, navi::Data *namedData) -> bool {                             \
        using T = std::remove_const_t<std::remove_pointer_t<decltype(this)> >; \
        auto *typedObj = static_cast<T *>(obj);                                \
        typedObj->member = nullptr;                                            \
        if (!namedData) {                                                      \
            return true;                                                       \
        }                                                                      \
        auto *typedData = dynamic_cast<decltype(typedObj->member)>(namedData); \
        typedObj->member = typedData;                                          \
        return typedData != nullptr;                                           \
    }
}

#endif //NAVI_DATA_H
