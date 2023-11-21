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
#include <string.h>
#include <unordered_set>

#include "autil/Lock.h"

namespace autil {

template <typename T>
struct alignas(64) AlignedCont {
    T value;
};

template <typename T, bool required = false>
class ObjectTracer {
protected:
    ObjectTracer() {
        if (!enabled()) {
            return;
        }
        size_t idx = getObjectShardIdx();
        {
            autil::ScopedLock lock(OBJECT_LOCKS[idx].value);
            assert(OBJECT_SETS[idx].value.count(this) == 0 && "duplicated object constructor");
            OBJECT_SETS[idx].value.insert(this);
        }
    }
    ~ObjectTracer() {
        if (!enabled()) {
            return;
        }
        size_t idx = getObjectShardIdx();
        {
            autil::ScopedLock lock(OBJECT_LOCKS[idx].value);
            assert(OBJECT_SETS[idx].value.count(this) == 1 && "duplicated object destructor");
            OBJECT_SETS[idx].value.erase(this);
        }
    }

private:
    size_t getObjectShardIdx() const { return (size_t)this % SHARD_SIZE; }
    static bool enabled() {
        static const bool flag = required || getEnableFromEnv();
        return flag;
    }
    static bool getEnableFromEnv() {
        const char *str = std::getenv("AutilTracerType");
        bool flag = false;
        if (str) {
            flag = strstr(str, "all") || strstr(str, typeid(T).name());
        }
        fprintf(stderr, "object tracer for type[%s] enabled[%d]\n", typeid(T).name(), flag);
        return flag;
    }

private:
    static constexpr size_t SHARD_SIZE = 13;
    static AlignedCont<autil::ThreadMutex> OBJECT_LOCKS[SHARD_SIZE];
    static AlignedCont<std::unordered_set<void *>> OBJECT_SETS[SHARD_SIZE];
};

template <typename T, bool required>
AlignedCont<std::unordered_set<void *>> ObjectTracer<T, required>::OBJECT_SETS[];
template <typename T, bool required>
AlignedCont<autil::ThreadMutex> ObjectTracer<T, required>::OBJECT_LOCKS[];

} // namespace autil
