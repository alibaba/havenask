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

#include <atomic>
#include <memory>

#include "autil/Lock.h"
#include "suez/drc/DrcConfig.h"

namespace autil {
class LoopThread;
using LoopThreadPtr = std::shared_ptr<LoopThread>;
} // namespace autil

namespace build_service::util {
class SwiftClientCreator;
using SwiftClientCreatorPtr = std::shared_ptr<SwiftClientCreator>;
} // namespace build_service::util

namespace indexlibv2::framework {
class ITablet;
}

namespace kmonitor {
class MetricsReporter;
using MetricsReporterPtr = std::shared_ptr<MetricsReporter>;
} // namespace kmonitor

namespace suez {

class LogReplicationPipeline;

class LogReplicator {
public:
    LogReplicator(const DrcConfig &config,
                  const build_service::util::SwiftClientCreatorPtr &swiftClientCreator,
                  std::shared_ptr<indexlibv2::framework::ITablet> index,
                  const kmonitor::MetricsReporterPtr &reporter = nullptr);
    virtual ~LogReplicator();

public:
    bool start();
    void stop();

private:
    void workLoop();
    void maybeCreatePipelines();
    virtual std::shared_ptr<LogReplicationPipeline> createPipeline(const DrcConfig &config) const;

private:
    DrcConfig _config;
    std::shared_ptr<build_service::util::SwiftClientCreator> _swiftClientCreator;
    std::shared_ptr<indexlibv2::framework::ITablet> _index;
    kmonitor::MetricsReporterPtr _metricsReporter;

    std::atomic<bool> _running;
    std::map<std::string, std::shared_ptr<LogReplicationPipeline>> _pipelines;
    mutable autil::ReadWriteLock _lock;
    autil::LoopThreadPtr _workThread;
};

} // namespace suez
