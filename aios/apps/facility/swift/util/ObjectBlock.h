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
#include <memory>
#include <new>
#include <stddef.h>
#include <stdint.h>

#include "autil/Log.h"
#include "swift/util/Block.h"

namespace swift {
namespace util {
template <class T>
class ObjectBlock {
public:
    ObjectBlock(BlockPtr block) {
        _block = block;
        size_t blockSize = block->getSize();
        char *buffer = block->getBuffer();
        assert(blockSize >= sizeof(T));
        _objectCount = blockSize / sizeof(T);
        _objects = new (buffer) T[_objectCount];
        _lastDestoryPos = -1;
    }
    ~ObjectBlock() {
        int64_t i = _lastDestoryPos + 1;
        for (; i < _objectCount; i++) {
            _objects[i].~T();
        }
    }

private:
    ObjectBlock(const ObjectBlock &);
    ObjectBlock &operator=(const ObjectBlock &);

public:
    int64_t size() { return _objectCount; }
    T &operator[](int64_t index) const {
        assert(index < _objectCount && index > _lastDestoryPos);
        return _objects[index];
    }
    void destoryBefore(int64_t pos) {
        assert(pos < _objectCount);
        if (pos <= _lastDestoryPos) {
            return;
        }
        int64_t i = _lastDestoryPos + 1;
        for (; i <= pos; i++) {
            _objects[i].~T();
        }
        _lastDestoryPos = pos;
    }

private:
    int64_t _objectCount;
    T *_objects;
    BlockPtr _block;
    int64_t _lastDestoryPos;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace util
} // namespace swift
