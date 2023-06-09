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
#include "suez/worker/AsyncExecutorFactory.h"

#include "alog/Logger.h"
#include "autil/Log.h"
#include "future_lite/Executor.h"
#include "future_lite/ExecutorCreator.h"

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, AsyncExecutorFactory);

std::unique_ptr<future_lite::Executor>
AsyncExecutorFactory::createAsyncExecutor(const std::string &name, size_t threadNum, const std::string &typeStr) {
    auto executor= future_lite::ExecutorCreator::Create(
        typeStr, future_lite::ExecutorCreator::Parameters().SetExecutorName(name).SetThreadNum(threadNum));

    if (!executor) {
        AUTIL_LOG(ERROR, "not support async executor type: %s", typeStr.c_str());
    }
    return executor;
}

} // namespace suez
