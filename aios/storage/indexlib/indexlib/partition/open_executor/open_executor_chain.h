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
#ifndef __INDEXLIB_OPEN_EXECUTOR_CHAIN_H
#define __INDEXLIB_OPEN_EXECUTOR_CHAIN_H

#include <memory>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/open_executor/open_executor.h"

namespace indexlib { namespace partition {

class OpenExecutorChain
{
public:
    OpenExecutorChain();
    virtual ~OpenExecutorChain();

public:
    bool Execute(ExecutorResource& resource);
    void PushBack(const OpenExecutorPtr& executor);

protected:
    void Drop(int32_t failId, ExecutorResource& resource);
    virtual void DropOne(int32_t idx, ExecutorResource& resource) { mExecutors[idx]->Drop(resource); }
    virtual bool ExecuteOne(int32_t idx, ExecutorResource& resource) { return mExecutors[idx]->Execute(resource); }

private:
    typedef std::vector<OpenExecutorPtr> OpenExecutorVec;
    OpenExecutorVec mExecutors;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OpenExecutorChain);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OPEN_EXECUTOR_CHAIN_H
