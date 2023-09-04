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

#include "worker_framework/WorkerState.h"

namespace worker_framework {

class EmptyState final : public WorkerState {
public:
    EmptyState() = default;
    ~EmptyState() = default;

public:
    ErrorCode write(const std::string &content) override { return WorkerState::EC_OK; }
    ErrorCode read(std::string &content) override { return WorkerState::EC_OK; }
};

} // namespace worker_framework
