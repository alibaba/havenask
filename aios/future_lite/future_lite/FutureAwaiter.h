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
#ifndef FUTURE_LITE_FUTURE_AWAITER_H
#define FUTURE_LITE_FUTURE_AWAITER_H

#include <type_traits>
#include <unistd.h>

#include "future_lite/Common.h"

namespace future_lite {

template <typename FutureType>
struct FutureAwaiter {
    using valueType = typename FutureType::value_type;
    FutureAwaiter(FutureType&& f) : _f(std::move(f)) {}
    FutureAwaiter(FutureAwaiter&) = delete;
    FutureAwaiter(FutureAwaiter&& rhs) : _f(std::move(rhs._f)) {}
#if FUTURE_LITE_COROUTINE_AVAILABLE
    bool await_ready() {
        if (_f.hasResult()) {
            return true;
        } else {
            return false;
        }
    }
    void await_suspend(std::coroutine_handle<> h) {
        auto func = [h](Try<valueType>&& t) mutable {
            h.resume();
        };
        std::move(_f).thenTry(std::move(func));
    }
    valueType await_resume() { return std::move(_f.value()); }
#endif
    operator valueType()&& { return std::move(_f).get(); }
    FutureType _f;
};

}  // namespace future_lite

#endif  //FUTURE_LITE_FUTURE_AWAITER_H
