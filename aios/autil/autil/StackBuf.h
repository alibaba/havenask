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
namespace autil {

template <class T>
class StackBuf {
public:
    StackBuf(size_t size) {
        _size = size;
        if (size > STACK_BUF_LEN) {
            _buf = new T[size];
        } else {
            _buf = _stackBuf;
        }
    }
    ~StackBuf() {
        if (_size > STACK_BUF_LEN) {
            delete[] _buf;
        }
    }

    T *getBuf() const { return _buf; }

private:
    static const size_t STACK_BUF_LEN = 4 * 1024 / sizeof(T);
    size_t _size;
    T _stackBuf[STACK_BUF_LEN];
    T *_buf;
};

} // namespace autil
