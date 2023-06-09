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
#include "indexlib/partition/open_executor/open_executor_chain.h"

#include <memory>

#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OpenExecutorChain);

OpenExecutorChain::OpenExecutorChain() {}

OpenExecutorChain::~OpenExecutorChain() {}

bool OpenExecutorChain::Execute(ExecutorResource& resource)
{
    int32_t idx = 0;
    try {
        for (; idx < (int32_t)mExecutors.size(); ++idx) {
            assert(mExecutors[idx]);
            if (!ExecuteOne(idx, resource)) {
                Drop(idx, resource);
                return false;
            }
        }
    } catch (const FileIOException& e) {
        resource.mNeedReload.store(true, std::memory_order_relaxed);
        // Drop(idx, resource);
        throw e;
    } catch (const ExceptionBase& e) {
        resource.mNeedReload.store(true, std::memory_order_relaxed);
        // Drop(idx, resource);
        throw e;
    } catch (const exception& e) {
        resource.mNeedReload.store(true, std::memory_order_relaxed);
        // Drop(idx, resource);
        throw e;
    }

    return true;
}

void OpenExecutorChain::Drop(int32_t failId, ExecutorResource& resource)
{
    IE_LOG(WARN, "open/reopen failed, fail id [%d], executor drop begin", failId);
    for (int i = failId; i >= 0; --i) {
        assert(mExecutors[i]);
        DropOne(i, resource);
    }
    IE_LOG(WARN, "executor drop end");
}

void OpenExecutorChain::PushBack(const OpenExecutorPtr& executor) { mExecutors.push_back(executor); }
}} // namespace indexlib::partition
