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

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "worker_base/CompressedWorkerStateImpl.h"
#include "worker_framework/EmptyState.h"
#include "worker_framework/LocalState.h"
#include "worker_framework/ZkState.h"

namespace worker_framework {
namespace worker_base {

AUTIL_DECLARE_AND_SETUP_LOGGER(worker_framework.worker_base, CompressedWorkerState);

const std::string ZK_PREFIX = "zfs://";
const std::string LOCAL_PREFIX = "LOCAL://";

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

std::unique_ptr<WorkerState> CompressedWorkerState::createWithFilePath(const std::string &filePath,
                                                                       autil::CompressType type) {
    if (autil::StringUtil::startsWith(filePath, ZK_PREFIX)) {
        auto state = ZkState::create(filePath);
        if (state == nullptr) {
            return nullptr;
        }
        std::unique_ptr<ZkState> zkState(state);
        return worker_base::CompressedWorkerState::create(std::move(zkState), type);
    } else if (autil::StringUtil::startsWith(filePath, LOCAL_PREFIX)) {
        auto localState = std::make_unique<LocalState>(filePath);
        return worker_base::CompressedWorkerState::create(std::move(localState), type);
    } else if (filePath.empty()) {
        return std::make_unique<EmptyState>();
    }
    return nullptr;
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
