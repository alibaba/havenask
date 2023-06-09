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
#include <type_traits>
#include "future_lite/uthread/Uthread.h"

namespace future_lite {
namespace uthread {

// Use async api to make c++ lambda run into uthread stackfull coroutine.

struct Launch {
    // Create a Uthread to a specifiy executor. The async function would return immediately.
    struct Schedule {};
    // Create a Uthread and execute it in current thread and detached.
    // The async function would return after the created Uthread checked out.
    struct Current {};
    // Create a Uthread and execute it in current thread and not detached.
    // We can call Uthread.join() to set callback, callback will invoked after uthread stoped.
    struct Prompt {};
};

// Usage Example:
// uthread::async<uthread::Launch::Schedule>(<lambda>, executor);
// uthread::async<uthread::Launch::Current>(<lambda>, executor);

template <class T, class F,
          typename std::enable_if<std::is_same<T, Launch::Prompt>::value,
                                  T>::type* = nullptr>
inline Uthread async(F&& f, Executor* ex) {
    return Uthread(Attribute{ex, internal::get_parent_css()}, std::forward<F>(f));
}

template <class T, class F,
          typename std::enable_if<std::is_same<T, Launch::Schedule>::value,
                                  T>::type* = nullptr>
inline void async(F&& f, Executor* ex) {
    if (FL_UNLIKELY(!ex)) {
        return;
    }
    CoroSpecStorage* parent = internal::get_parent_css();
    ex->schedule([f = std::move(f), ex, parent]() {
        Uthread uth(Attribute{ex, parent}, std::move(f));
        uth.detach();
    });
}

// schedule async task, set a callback
template <class T, class F, class C,
            typename std::enable_if<std::is_same<T, Launch::Schedule>::value,
                                    T>::type* = nullptr>
inline void async(F&& f, C&& c, Executor* ex) {
    if (FL_UNLIKELY(!ex)) {
        return;
    }
    CoroSpecStorage* parent = internal::get_parent_css();
    ex->schedule([f = std::move(f), c = std::move(c), ex, parent]() {
        Uthread uth(Attribute{ex, parent}, std::move(f));
        uth.join(std::move(c));
    });
}

template <class T, class F,
            typename std::enable_if<std::is_same<T, Launch::Current>::value,
                                    T>::type* = nullptr>
inline void async(F&& f, Executor* ex) {
    Uthread uth(Attribute{ex, internal::get_parent_css()}, std::forward<F>(f));
    uth.detach();
}

} // namespace uthread
} // namespace future_lite
