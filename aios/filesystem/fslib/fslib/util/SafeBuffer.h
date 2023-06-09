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
#ifndef FSLIB_SAFEBUFFER_H
#define FSLIB_SAFEBUFFER_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"

FSLIB_BEGIN_NAMESPACE(util);

class SafeBuffer
{
public:
    SafeBuffer(int64_t size)
        : _size(size)
    {
        _buffer = new char[size];
    }
    
    ~SafeBuffer() {
        if (_buffer) {
            delete[] _buffer;
            _buffer = NULL;
        }
    }

public:
    char* getBuffer() const {
        return _buffer;
    }

    int64_t getSize() const {
        return _size;
    }

private:
    char* _buffer;
    int64_t _size;
};

FSLIB_TYPEDEF_AUTO_PTR(SafeBuffer);

FSLIB_END_NAMESPACE(util);

#endif //FSLIB_SAFEBUFFER_H
