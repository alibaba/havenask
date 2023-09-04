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

#include <functional>

#include "autil/WorkItem.h"

namespace autil {

class LambdaWorkItem : public autil::WorkItem {
public:
    LambdaWorkItem(std::function<void()> func) : _func(std::move(func)) {}

public:
    void process() override {
        if (_func) {
            _func();
        }
    }

private:
    std::function<void()> _func;
};

} // namespace autil
