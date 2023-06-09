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

#include "autil/mem_pool/PoolBase.h"

namespace autil {

template <typename PoolType, typename T, bool deallocate = false>
struct PoolDeleter {
    void operator()(T *p) {
        POOL_COMPATIBLE_DELETE_CLASS(pool, p);
        if constexpr (deallocate) {
            if (pool) {
                pool->deallocate((void *)p, sizeof(T));
            }
        }
    }
    PoolType *pool = nullptr;
};

template <typename PoolType, typename _Tp>
auto unique_ptr_pool(PoolType *pool, _Tp *p) {
    return std::unique_ptr<_Tp, PoolDeleter<PoolType, _Tp, false>>(p, PoolDeleter<PoolType, _Tp>{pool});
}

template <typename PoolType, typename _Tp>
std::shared_ptr<_Tp> shared_ptr_pool(PoolType *pool, _Tp *p) {
    return std::shared_ptr<_Tp>(p, PoolDeleter<PoolType, _Tp>{pool});
}

template <typename PoolType, typename _Tp>
auto unique_ptr_pool_deallocated(PoolType *pool, _Tp *p) {
    return std::unique_ptr<_Tp, PoolDeleter<PoolType, _Tp, true>>(p, PoolDeleter<PoolType, _Tp, true>{pool});
}

template <typename PoolType, typename _Tp>
std::shared_ptr<_Tp> shared_ptr_pool_deallocated(PoolType *pool, _Tp *p) {
    return std::shared_ptr<_Tp>(p, PoolDeleter<PoolType, _Tp, true>{pool});
}

template <typename To, typename From, typename Deleter>
std::unique_ptr<To, Deleter> dynamic_unique_cast(std::unique_ptr<From, Deleter> &&p) {
    if (To *cast = dynamic_cast<To *>(p.get())) {
        std::unique_ptr<To, Deleter> result(cast, std::move(p.get_deleter()));
        p.release();
        return result;
    }
    return std::unique_ptr<To, Deleter>(nullptr, p.get_deleter());
}

} // namespace autil
