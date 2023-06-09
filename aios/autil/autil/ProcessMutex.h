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

#include <filesystem>
#include <memory>

#include "autil/result/Result.h"

namespace autil {

class ProcessMutex {
public:
    void release() { _lt.reset(); }

    static Result<ProcessMutex> acquire(const std::filesystem::path &path);

private:
    ProcessMutex(const std::shared_ptr<void> &lt) : _lt{lt} {}

private:
    std::shared_ptr<void> _lt;
};

} // namespace autil
