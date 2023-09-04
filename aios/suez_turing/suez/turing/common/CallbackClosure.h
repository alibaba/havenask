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
#include "suez/turing/common/Closure.h"

namespace suez {
namespace turing {

class CallbackClosure : public Closure {
public:
    typedef std::function<void(void)> Func;

public:
    CallbackClosure(Func callback) : _callback(std::move(callback)) {}
    ~CallbackClosure() {}
    CallbackClosure(const CallbackClosure &) = delete;
    CallbackClosure &operator=(const CallbackClosure &) = delete;

public:
    void run() override { _callback(); }

private:
    Func _callback;
};

} // namespace turing
} // namespace suez
