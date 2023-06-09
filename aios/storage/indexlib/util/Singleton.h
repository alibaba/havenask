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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/base/Define.h"

namespace indexlib { namespace util {

class LazyInstantiation
{
protected:
    template <typename T>
    static void Create(T*& ptr)
    {
        T* tmp = new T;
        MEMORY_BARRIER();
        ptr = tmp;
        static std::shared_ptr<T> destroyer(ptr);
    }
};

template <typename T, typename InstPolicy = LazyInstantiation>
class Singleton : private InstPolicy
{
protected:
    Singleton(const Singleton&) {}
    Singleton() {}

public:
    ~Singleton() {}

public:
    /**
     * Provide access to the single instance through double-checked locking
     *
     * @return the single instance of object.
     */
    static T* GetInstance();

private:
    AUTIL_LOG_DECLARE();
};

template <typename T, typename InstPolicy>
inline T* Singleton<T, InstPolicy>::GetInstance()
{
    static T* ptr = 0;
    static autil::RecursiveThreadMutex gLock;

    if (unlikely(!ptr)) {
        autil::ScopedLock sl(gLock);
        if (!ptr) {
            InstPolicy::Create(ptr);
        }
    }
    return const_cast<T*>(ptr);
}
}} // namespace indexlib::util
