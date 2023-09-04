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

#include <memory>

#include "suez/drc/DrcConfig.h"

namespace build_service::util {
class SwiftClientCreator;
}

namespace indexlibv2::framework {
class ITablet;
}

namespace kmonitor {
class MetricsReporter;
using MetricsReporterPtr = std::shared_ptr<MetricsReporter>;
} // namespace kmonitor

namespace worker_framework {
class WorkerState;
}

namespace suez {

class LogReplicationPipeline;
class Source;
class Sink;
class LogWriter;

using SwiftClientCreatorPtr = std::shared_ptr<build_service::util::SwiftClientCreator>;

class LogReplicatorCreator {
public:
    static std::unique_ptr<LogReplicationPipeline> create(const DrcConfig &config,
                                                          const SwiftClientCreatorPtr &swiftClientCreator,
                                                          const std::shared_ptr<indexlibv2::framework::ITablet> &index,
                                                          const kmonitor::MetricsReporterPtr &reporter = nullptr);

private:
    static std::unique_ptr<Source> createSource(const SwiftClientCreatorPtr &swiftClientCreator,
                                                const SourceConfig &sourceConfig);
    static std::unique_ptr<Sink> createSink(const SwiftClientCreatorPtr &swiftClientCreator,
                                            const SinkConfig &sinkConfig);
    static std::unique_ptr<LogWriter> createWriter(const SwiftClientCreatorPtr &swiftClientCreator,
                                                   const SinkConfig &sinkConfig);
    static std::unique_ptr<worker_framework::WorkerState> createState(const std::string &stateFile);
};

} // namespace suez
