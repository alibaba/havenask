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

#include <vector>

#include "autil/Lock.h"
#include "cava/jit/CavaJit.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {

struct CavaJitPoolItem {
    cava::CavaJit cavaJit;
};

SUEZ_TYPEDEF_PTR(CavaJitPoolItem);

class CavaJitPool {
public:
    CavaJitPool();
    ~CavaJitPool();

    CavaJitPool(const CavaJitPool &) = delete;
    CavaJitPool &operator=(const CavaJitPool &) = delete;

public:
    void initAdd(CavaJitPoolItem *item);
    CavaJitPoolItemPtr get();

private:
    void put(CavaJitPoolItem *item);

private:
    autil::SpinLock _lock;
    std::vector<CavaJitPoolItem *> _poolItemVec;
};

SUEZ_TYPEDEF_PTR(CavaJitPool);

} // namespace turing
} // namespace suez
