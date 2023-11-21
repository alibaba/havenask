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

#include "autil/Lock.h"
#include "build_service/common_define.h"

namespace build_service { namespace util {

template <typename T>
class SharedPtrGuard
{
public:
    SharedPtrGuard() {}

    SharedPtrGuard(const std::shared_ptr<T>& dataPtr) : _dataPtr(dataPtr) {}
    ~SharedPtrGuard() {};

private:
    SharedPtrGuard(const SharedPtrGuard&);
    SharedPtrGuard& operator=(const SharedPtrGuard&);

public:
    void set(const std::shared_ptr<T>& inDataPtr)
    {
        autil::ScopedLock lock(_mutex);
        _dataPtr = inDataPtr;
    }

    void get(std::shared_ptr<T>& outDataPtr) const
    {
        autil::ScopedLock lock(_mutex);
        outDataPtr = _dataPtr;
    }

    void reset()
    {
        autil::ScopedLock lock(_mutex);
        _dataPtr.reset();
    }

private:
    std::shared_ptr<T> _dataPtr;
    mutable autil::ThreadMutex _mutex;
};

}} // namespace build_service::util
