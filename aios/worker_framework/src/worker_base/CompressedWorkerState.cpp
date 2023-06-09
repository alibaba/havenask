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
#include "worker_framework/CompressedWorkerState.h"

#include "worker_base/CompressedWorkerStateImpl.h"

namespace worker_framework {
namespace worker_base {

CompressedWorkerState::CompressedWorkerState(std::unique_ptr<WorkerState> underlying)
    : _underlying(std::move(underlying)) {}

CompressedWorkerState::~CompressedWorkerState() {}

WorkerState::ErrorCode CompressedWorkerState::write(const std::string &content) {
    std::string compressed;
    if (!compress(content, compressed)) {
        return WorkerState::EC_FAIL;
    }
    return _underlying->write(compressed);
}

WorkerState::ErrorCode CompressedWorkerState::read(std::string &content) {
    std::string compressed;
    auto ec = _underlying->read(compressed);
    if (ec == WorkerState::EC_OK || ec == WorkerState::EC_UPDATE) {
        if (!decompress(compressed, content)) {
            ec = WorkerState::EC_FAIL;
        }
    }
    return ec;
}

std::unique_ptr<WorkerState> CompressedWorkerState::create(std::unique_ptr<WorkerState> underlying,
                                                           autil::CompressType type) {
    if (type >= autil::CompressType::MAX || type <= autil::CompressType::NO_COMPRESS) {
        return underlying;
    }
    return std::make_unique<CompressedWorkerStateImpl>(std::move(underlying), type);
}

} // namespace worker_base
} // namespace worker_framework
