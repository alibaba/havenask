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

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "matchdoc/Trait.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "table/Table.h"

namespace navi {
class GraphMemoryPoolResource;
} // namespace navi

namespace isearch {
namespace sql {

class SqlQueryResource;

#define KERNEL_REQUIRES(ptr, msg)                                                                  \
    if ((ptr) == nullptr) {                                                                        \
        SQL_LOG(ERROR, msg);                                                                       \
        return navi::EC_ABORT;                                                                     \
    }

class KernelUtil {
public:
    KernelUtil() {}
    ~KernelUtil() {}

public:
    static void stripName(std::string &name);
    static void stripName(std::vector<std::string> &vec);
    template <typename T>
    static void stripName(std::map<std::string, T> &kv);
    template <typename T>
    static void stripName(T &value);
    static table::TablePtr getTable(const navi::DataPtr &data);
    static table::TablePtr getTable(const navi::DataPtr &data,
                                    navi::GraphMemoryPoolResource *memoryPoolResource);
    static table::TablePtr getTable(const navi::DataPtr &data,
                                    navi::GraphMemoryPoolResource *memoryPoolResource,
                                    bool needCopy);

public:
    static const std::string FIELD_PREIFX;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
void KernelUtil::stripName(std::map<std::string, T> &kv) {
    std::map<std::string, T> newMap = {};
    for (auto &pair : kv) {
        std::string key = pair.first;
        stripName(key);
        T &value = pair.second;
        stripName(value);
        newMap.insert(std::make_pair(key, std::move(value)));
    }
    kv = std::move(newMap);
}

template <typename T>
void KernelUtil::stripName(T &value) {}

template <typename T, bool = matchdoc::ConstructTypeTraits<T>::NeedConstruct()>
struct InitializeIfNeeded {
    inline void operator()(T &val) {}
};

template <typename T>
struct InitializeIfNeeded<T, false> {
    inline void operator()(T &val) {
        val = 0;
    }
};

} // namespace sql
} // namespace isearch
