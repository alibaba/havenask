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

namespace autil {

template <typename Lambda>
class [[nodiscard]] Defer {
public:
    explicit Defer(Lambda &&f) : _f(std::forward<Lambda>(f)) {}

    Defer(Defer &&other) : _f(std::forward<Lambda>(other._f)) { other.release(); }

    Defer(const Defer &) = delete;
    Defer &operator=(const Defer &) = delete;
    Defer &operator=(Defer &&) = delete;

    ~Defer() {
        if (_run) {
            _f();
        }
    }

    void release() noexcept { _run = false; }

private:
    [[no_unique_address]] Lambda _f;
    bool _run = true;
};

template <typename Lambda>
Defer(Lambda) -> Defer<Lambda>;

} // namespace autil
