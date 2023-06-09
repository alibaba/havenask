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
#include "future_lite/uthread/internal/thread.h"

namespace future_lite {
namespace uthread {

class Attribute {
public:
    Executor* ex;
    CoroSpecStorage* parent = nullptr;
};

class Uthread {
public:
    Uthread() = default;
    template <class Func>
    Uthread(Attribute attr, Func&& func)
        : _attr(std::move(attr)) {
        _ctx = std::make_unique<internal::thread_context>(std::move(func), _attr.ex, _attr.parent);
    }
    ~Uthread() = default;
    Uthread(Uthread&& x) noexcept = default;
    Uthread& operator=(Uthread&& x) noexcept = default;

public:
    template <class Callback>
    bool join(Callback&& callback) {
        if (!_ctx || _ctx->joined_) {
            return false;
        }
        _ctx->joined_ = true;
        auto f = _ctx->done_.getFuture();
        if (f.hasResult()) {
            callback();
            return true;
        }
        if (!f.getExecutor()) {
            // we can not delay the uthread life without executor.
            // so, if the user do not hold the uthread in outside,
            // the user can not do switch in again.
            std::move(f).setContinuation(
                [callback = std::move(callback)](auto&&){ callback(); });
        } else {
            _ctx->done_.forceSched().checkout();
            std::move(f).setContinuation(
                [callback = std::move(callback),
                 // hold on the life of uthread.
                 // user never care about the uthread's destruct.
                 self = std::move(*this)](auto&&){ callback(); });
        }
        return true;
    }
    void detach() {
        join([](){});
    }

private:
    Attribute _attr;
    std::unique_ptr<internal::thread_context> _ctx;
};

inline Executor* getCurrentExecutor() {
    auto ctx = uthread::internal::thread_impl::get();
    if (!ctx) {
        return nullptr;
    }
    return ctx->done_.getExecutor();
}

inline bool onUthread() {
    return uthread::internal::thread_impl::can_switch_out();
}

} // namespace uthread
} // namespace future_lite
