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
#include "autil/ShortString.h"

namespace autil {

ShortString& ShortString::operator += (const ShortString &addStr) {
    size_t addSize = addStr.size();
    if (_size + addSize >= BUF_SIZE) {
        char *newBuf = new char[_size + addSize + 1];
        memcpy(newBuf, _data, _size);
        memcpy(newBuf + _size, addStr.c_str(), addSize);
        if (_size >= BUF_SIZE) {
            delete [] _data;
        }
        _size += addSize;
        newBuf[_size] = '\0';
        _data = newBuf;
    } else {
        memcpy(_buf + _size, addStr.c_str(), addSize);
        _size += addSize;
        _buf[_size] = '\0';
    }
    return *this;
}

}
