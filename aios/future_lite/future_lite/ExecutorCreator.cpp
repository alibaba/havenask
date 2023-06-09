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
#include "future_lite/ExecutorCreator.h"

#include "future_lite/Executor.h"

namespace future_lite {

ExecutorCreator __attribute__((init_priority(101))) ExecutorCreator::globalInstance;

namespace detail {

void ExecutorCreatorBase::Register(const std::string &type, std::unique_ptr<detail::ExecutorCreatorBase> creator) {
    auto &instance = ExecutorCreator::Instance();
    instance.Register(type, std::move(creator));
}

} // namespace detail

ExecutorCreator &ExecutorCreator::Instance() { return globalInstance; }

std::unique_ptr<future_lite::Executor>
ExecutorCreator::DoCreate(const std::string &type, const detail::ExecutorCreatorBase::Parameters &parameters) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto iter = _creators.find(type);
    if (iter == _creators.end()) {
        std::fprintf(stderr, "executor type[%s] not registered\n", type.c_str());
        return nullptr;
    }
    return iter->second->Create(parameters);
}

bool ExecutorCreator::DoHasExecutor(const std::string &type) const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _creators.find(type)!=_creators.end();
}

void ExecutorCreator::Register(const std::string &type, std::unique_ptr<detail::ExecutorCreatorBase> creator) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto iter = _creators.find(type);
    if (iter != _creators.end()) {
        std::fprintf(stderr, "executor type[%s] already registed\n", type.c_str());
#ifndef NDEBUG
        std::abort();
#endif
    }
    _creators.insert({type, std::move(creator)});
}

} // namespace future_lite
