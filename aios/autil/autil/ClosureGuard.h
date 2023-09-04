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

#include "google/protobuf/stubs/common.h"

namespace autil {

class ClosureGuard {
public:
    explicit ClosureGuard(::google::protobuf::Closure *done) : _done(done) {}
    ~ClosureGuard() {
        if (_done != nullptr) {
            _done->Run();
        }
    }

public:
    ::google::protobuf::Closure *steal() {
        ::google::protobuf::Closure *done = _done;
        _done = nullptr;
        return done;
    }

private:
    ClosureGuard(const ClosureGuard &);
    ClosureGuard &operator=(const ClosureGuard &);

public:
    ::google::protobuf::Closure *_done;
};

} // namespace autil
