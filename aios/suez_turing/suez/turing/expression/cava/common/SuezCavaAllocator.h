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

#include <memory>
#include <new>
#include <stddef.h>

#include "autil/CommonMacros.h"
#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "cava/common/Common.h"
#include "cava/runtime/Allocator.h"
#include "cava/runtime/CString.h"
#include "cava/runtime/CavaArrayType.h"

class CavaCtx;
namespace matchdoc {
class ReferenceBase;
} // namespace matchdoc

namespace suez {
namespace turing {

class SuezCavaAllocator : public cava::Allocator {
public:
    SuezCavaAllocator(autil::mem_pool::Pool *pool, size_t allocSizeLimit)
        : _pool(pool), _allocCount(0), _allocSize(0), _allocSizeLimit(allocSizeLimit) {}
    ~SuezCavaAllocator() { destroy(); }

private:
    SuezCavaAllocator(const SuezCavaAllocator &);
    SuezCavaAllocator &operator=(const SuezCavaAllocator &);

public:
    autil::mem_pool::Pool *getPool() { return _pool; }
    size_t getAllocCount() { return _allocCount; }
    size_t getAllocSize() { return _allocSize; }

protected:
    void *alloc(size_t size) override {
        size_t alignSize = autil::mem_pool::Pool::alignBytes(size, _pool->getAlignSize());
        if (!tryIncAllocSize(alignSize)) {
            return NULL;
        }
        ++_allocCount;
        return _pool->allocate(size);
    }
    bool tryIncAllocSize(size_t size) override {
        if (unlikely(_allocSize + size > _allocSizeLimit)) {
            return false;
        }
        _allocSize += size;
        return true;
    }

private:
    autil::mem_pool::Pool *_pool;
    size_t _allocCount;
    size_t _allocSize;
    size_t _allocSizeLimit;
};

typedef std::shared_ptr<SuezCavaAllocator> SuezCavaAllocatorPtr;

class SuezCavaAllocUtil : public cava::AllocUtil {
public:
    using cava::AllocUtil::allocCString;
    static cava::lang::CString *allocCString(CavaCtx *ctx, const autil::StringView *str) {
        auto ret = (cava::lang::CString *)alloc(ctx, sizeof(cava::lang::CString));
        auto bytes = (CavaByteArray)alloc(ctx, sizeof(CavaArrayType<cava::byte>));
        if (!ret || !bytes) {
            return nullptr;
        }
        bytes->length = str->size();
        bytes->setData((cava::byte *)str->data());
        new (ret) cava::lang::CString(bytes, NULL); //这个地方有差异
        return ret;
    }

    template <typename T>
    static CavaArrayType<T> *allocCavaArray(CavaCtx *ctx, uint size) {
        CavaArrayType<T> *array = (CavaArrayType<T> *)alloc(ctx, sizeof(CavaArrayType<T>));
        if (unlikely(array == NULL)) {
            return NULL;
        }
        array->length = size;
        if (size > 0) {
            T *data = (T *)alloc(ctx, sizeof(T) * size);
            if (unlikely(data == NULL)) {
                return NULL;
            }
            array->setData(data);
        } else {
            array->setData(NULL);
        }
        return array;
    }

    template <class RefType>
    static RefType *allocRef(CavaCtx *ctx, matchdoc::ReferenceBase *ref, bool isConst) {
        RefType *ret = (RefType *)alloc(ctx, sizeof(RefType));
        if (unlikely(ret == NULL)) {
            return NULL;
        }
        new (ret) RefType(ref, isConst);
        return ret;
    }
};

} // namespace turing
} // namespace suez
