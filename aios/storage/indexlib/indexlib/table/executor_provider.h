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

#include <string>

#include "future_lite/Executor.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace table {

class ExecutorProvider
{
public:
    ExecutorProvider();
    virtual ~ExecutorProvider();

    ExecutorProvider(const ExecutorProvider&) = delete;
    ExecutorProvider& operator=(const ExecutorProvider&) = delete;
    ExecutorProvider(ExecutorProvider&&) = delete;
    ExecutorProvider& operator=(ExecutorProvider&&) = delete;

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options);
    virtual bool DoInit() = 0;
    virtual std::string GetExecutorName() const = 0;
    virtual future_lite::Executor* CreateExecutor() const = 0;

protected:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExecutorProvider);
}} // namespace indexlib::table
