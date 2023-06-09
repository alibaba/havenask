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

#include <type_traits>
#include "future_lite/Future.h"

namespace future_lite {
namespace uthread {

// Latch latch(2);
// latch.await();
//     latch.downCount();
//     latch.downCount();

class Latch {
public:
    explicit Latch(std::size_t count)
        : _count(count), _skip(!count) {
    }
    Latch(const Latch&) = delete;
    Latch(Latch&&) = delete;

    ~Latch() {}

public:
    void downCount(std::size_t n = 1) {
        if (_skip) {
            return;
        }
        auto lastCount = _count.fetch_sub(n, std::memory_order_acq_rel);
        if (lastCount == 1u) {
            _promise.setValue(true);
        }
    }
    void await(Executor* ex) {
        if (_skip) {
            return;
        }
        _promise.getFuture().via(ex).get();
    }
    std::size_t currentCount() const {
        return _count.load(std::memory_order_acquire);
    }

private:
    Promise<bool> _promise;
    std::atomic<std::size_t> _count;
    bool _skip;
};

} // namespace uthread
} // namespace future_lite
