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
#ifndef FSLIB_SINGLETON_H
#define FSLIB_SINGLETON_H

#include "autil/Log.h"
#include "autil/Lock.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"

FSLIB_BEGIN_NAMESPACE(util);

class LazyInstantiation
{
protected:
    template<typename T>
    static void create(T*& ptr)
    {
        ptr = new T;
        static std::unique_ptr<T> destroyer(ptr);
    }

};

template<typename T, typename InstPolicy = LazyInstantiation>
class Singleton : private InstPolicy
{
protected:
    Singleton(const Singleton&) {}
    Singleton(){}

public:
    ~Singleton(){}

public:
    /**
     * Provide access to the single instance through double-checked locking
     *
     * @return the single instance of object.
     */
    static T* getInstance();
};

template<typename T, typename InstPolicy>
T* Singleton<T, InstPolicy>::getInstance()
{
    static T* ptr = 0;
    static autil::ThreadMutex gLock;

    if (!ptr)
    {
        autil::ScopedLock lock(gLock);
        if (!ptr)
        {
            InstPolicy::create(ptr);
        }
    }
    return const_cast<T*>(ptr);
}

FSLIB_END_NAMESPACE(util);

#endif //FSLIB_SINGLETON_H
