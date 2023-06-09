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
#include <stddef.h>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "cava/common/Common.h"
#include "cava/common/ErrorDef.h"
#include "cava/runtime/CString.h"
#include "cava/runtime/CavaArrayType.h"
#include "cava/runtime/CavaCtx.h"
#include <functional>

extern "C" {
    // flag != 0 memset alloc
    void *_cava_alloc_(CavaCtx *ctx, size_t size, int flag);
}

namespace cava {

class Allocator
{
public:
    Allocator() {}
    virtual ~Allocator() {
        assert(_destroyList.empty());
    }
    Allocator(const Allocator &) = delete;
    Allocator& operator=(Allocator &) = delete;
protected:
    virtual void *alloc(size_t size) = 0;
    virtual bool tryIncAllocSize(size_t size) {
        return false;
    }
    void destroy() {
    }
private:
    autil::ThreadMutex _mutex;
    std::vector<std::function<void()> > _destroyList;
};


class AllocUtil // raise exception when alloc failed
{
public:
    static void *alloc(CavaCtx *ctx, size_t size) {
        return nullptr;
    }

    static bool tryIncAllocSize(CavaCtx *ctx, size_t size) {
        return false;
    }

    static cava::lang::CString *allocCString(CavaCtx *ctx, std::string *holdStr) {
        return nullptr;
    }
    template<typename T, typename...U>
    static T *createNativeObject(CavaCtx *ctx, U&&... u) {
        auto *obj = (T *)alloc(ctx, sizeof(T));
        return obj;
    }
};

}
