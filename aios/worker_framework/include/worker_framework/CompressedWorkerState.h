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

#include "autil/CompressionUtil.h"
#include "worker_framework/WorkerState.h"

namespace worker_framework {
namespace worker_base {

class CompressedWorkerState : public WorkerState {
public:
    explicit CompressedWorkerState(std::unique_ptr<WorkerState> underlying);
    virtual ~CompressedWorkerState();

public:
    ErrorCode write(const std::string &content) override;
    ErrorCode read(std::string &content) override;

protected:
    virtual bool compress(const std::string &input, std::string &output) = 0;
    virtual bool decompress(const std::string &input, std::string &output) = 0;

public:
    static std::unique_ptr<WorkerState> create(std::unique_ptr<WorkerState> underlying, autil::CompressType type);

private:
    std::unique_ptr<WorkerState> _underlying;
};

} // namespace worker_base
} // namespace worker_framework
