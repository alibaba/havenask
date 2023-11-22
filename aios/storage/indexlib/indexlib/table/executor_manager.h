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

#include <map>
#include <memory>
#include <stddef.h>
#include <string>

#include "autil/Lock.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace future_lite {
class Executor;
}

namespace indexlib { namespace table {

class ExecutorProvider;

class ExecutorManager
{
public:
    ExecutorManager();
    ~ExecutorManager();

    ExecutorManager(const ExecutorManager&) = delete;
    ExecutorManager& operator=(const ExecutorManager&) = delete;
    ExecutorManager(ExecutorManager&&) = delete;
    ExecutorManager& operator=(ExecutorManager&&) = delete;

public:
    std::shared_ptr<future_lite::Executor> RegisterExecutor(const std::shared_ptr<ExecutorProvider>& provider);
    size_t ClearUselessExecutors();

private:
    mutable autil::ThreadMutex mMapLock;
    std::map<std::string, std::shared_ptr<future_lite::Executor>> mExecutors;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExecutorManager);
}} // namespace indexlib::table
