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

#include "navi/engine/Resource.h"

namespace future_lite {
class Executor;
}

namespace suez_navi {

class AsyncExecutorR : public navi::RootResource
{
public:
    AsyncExecutorR();
    AsyncExecutorR(future_lite::Executor *asyncInterExecutor,
                   future_lite::Executor *asyncIntraExecutor);
    ~AsyncExecutorR();
    AsyncExecutorR(const AsyncExecutorR &) = delete;
    AsyncExecutorR &operator=(const AsyncExecutorR &) = delete;
public:
    future_lite::Executor *getAsyncInterExecutor() const {
        return _asyncInterExecutor;
    }
    future_lite::Executor *getAsyncIntraExecutor() const {
        return _asyncIntraExecutor;
    }
public:
    static const std::string RESOURCE_ID;
private:
    future_lite::Executor *_asyncInterExecutor;
    future_lite::Executor *_asyncIntraExecutor;
};

NAVI_TYPEDEF_PTR(AsyncExecutorR);

}

