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
#include <algorithm>
#include <memory>
#include <stddef.h>
#include <stdint.h>

#include "autil/Thread.h"
#include "build_service/builder/Builder.h"
#include "build_service/common_define.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "build_service/util/MemControlStreamQueue.h"
#include "build_service/util/StreamQueue.h"
#include "fslib/fs/FileLock.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

namespace build_service { namespace builder {

class AsyncBuilder : public Builder
{
public:
    AsyncBuilder(const indexlib::partition::IndexBuilderPtr& indexBuilder, fslib::fs::FileLock* fileLock = NULL,
                 const proto::BuildId& buildId = proto::BuildId());
    /*override*/ ~AsyncBuilder();

private:
    AsyncBuilder(const AsyncBuilder&);
    AsyncBuilder& operator=(const AsyncBuilder&);

public:
    /*override*/ bool init(const config::BuilderConfig& builderConfig,
                           indexlib::util::MetricProviderPtr metricProvider = indexlib::util::MetricProviderPtr());
    // only return false when exception
    /*override*/ bool build(const indexlib::document::DocumentPtr& doc);
    /*override*/ bool merge(const indexlib::config::IndexPartitionOptions& options);
    /*override*/ void stop(int64_t stopTimestamp = INVALID_TIMESTAMP);
    /*override*/ bool isFinished() const { return !_running || _docQueue->empty(); }

private:
    void buildThread();
    void releaseDocs();
    void clearQueue();

private:
    typedef util::MemControlStreamQueue<indexlib::document::DocumentPtr> DocQueue;
    typedef util::StreamQueue<indexlib::document::DocumentPtr> Queue;
    typedef std::unique_ptr<DocQueue> DocQueuePtr;
    typedef std::unique_ptr<Queue> QueuePtr;

    volatile bool _running;
    autil::ThreadPtr _asyncBuildThreadPtr;
    DocQueuePtr _docQueue;
    QueuePtr _releaseDocQueue;

    indexlib::util::MetricPtr _asyncQueueSizeMetric;
    indexlib::util::MetricPtr _asyncQueueMemMetric;
    indexlib::util::MetricPtr _releaseQueueSizeMetric;

private:
    friend class AsyncBuilderTest;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AsyncBuilder);

}} // namespace build_service::builder
