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
#include "suez/drc/LogReplicatorCreator.h"

#include "autil/HashFuncFactory.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "build_service/util/SwiftClientCreator.h"
#include "indexlib/framework/ITablet.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/drc/Checkpoint.h"
#include "suez/drc/LogReader.h"
#include "suez/drc/LogReplicationPipeline.h"
#include "suez/drc/LogWriter.h"
#include "suez/drc/SwiftSink.h"
#include "suez/drc/SwiftSource.h"
#include "worker_framework/LocalState.h"
#include "worker_framework/ZkState.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez.drc, LogReplicatorCreator);

std::unique_ptr<LogReplicationPipeline>
LogReplicatorCreator::create(const DrcConfig &config,
                             const SwiftClientCreatorPtr &swiftClientCreator,
                             const std::shared_ptr<indexlibv2::framework::ITablet> &index,
                             const kmonitor::MetricsReporterPtr &reporter) {
    // reader
    const auto &sourceConfig = config.getSourceConfig();
    auto source = createSource(swiftClientCreator, sourceConfig);
    if (!source) {
        AUTIL_LOG(ERROR, "create source from %s failed", ToJsonString(sourceConfig).c_str());
        return nullptr;
    }
    auto reader = std::make_unique<LogReader>(std::move(source));

    // writers
    std::map<std::string, std::unique_ptr<LogWriter>> writers;
    const auto &sinkConfigs = config.getSinkConfigs();
    for (const auto &sinkConfig : sinkConfigs) {
        auto writer = createWriter(swiftClientCreator, sinkConfig);
        if (!writer) {
            return nullptr;
        }
        writers.emplace(sinkConfig.name, std::move(writer));
    }

    // checkpoint state
    auto checkpointFile = Checkpoint::getCheckpointFileName(config.getCheckpointRoot());
    auto state = createState(checkpointFile);
    if (!state) {
        AUTIL_LOG(ERROR, "create worker state for %s failed", checkpointFile.c_str());
        return nullptr;
    }

    auto pipeline = std::make_unique<LogReplicationPipeline>(
        std::move(reader), std::move(writers), index, std::move(state), reporter);
    if (!config.shareMode()) {
        pipeline->setName(sinkConfigs[0].name);
    }
    return pipeline;
}

std::unique_ptr<Source> LogReplicatorCreator::createSource(const SwiftClientCreatorPtr &swiftClientCreator,
                                                           const SourceConfig &sourceConfig) {
    if (sourceConfig.type != SwiftSource::TYPE) {
        AUTIL_LOG(ERROR, "only support swift source now");
        return nullptr;
    }
    return SwiftSource::create(swiftClientCreator, sourceConfig);
}

std::unique_ptr<LogWriter> LogReplicatorCreator::createWriter(const SwiftClientCreatorPtr &swiftClientCreator,
                                                              const SinkConfig &sinkConfig) {
    const auto &distConfig = sinkConfig.distConfig;
    if (distConfig._hashFields.empty()) {
        AUTIL_LOG(ERROR, "hash field is empty");
        return nullptr;
    }
    auto hashFunc = autil::HashFuncFactory::createHashFunc(distConfig._hashFunction, distConfig._hashParams);
    if (!hashFunc) {
        AUTIL_LOG(ERROR, "create hash function %s failed", distConfig._hashFunction.c_str());
        return nullptr;
    }
    auto sink = createSink(swiftClientCreator, sinkConfig);
    if (!sink) {
        AUTIL_LOG(ERROR, "create sink from %s failed", ToJsonString(sinkConfig).c_str());
        return nullptr;
    }
    return std::make_unique<LogWriter>(std::move(sink), std::move(hashFunc), distConfig._hashFields);
}

std::unique_ptr<Sink> LogReplicatorCreator::createSink(const SwiftClientCreatorPtr &swiftClientCreator,
                                                       const SinkConfig &sinkConfig) {
    if (sinkConfig.type != SwiftSink::TYPE) {
        AUTIL_LOG(ERROR, "only support swift sink now");
        return nullptr;
    }
    return SwiftSink::create(swiftClientCreator, sinkConfig);
}

using WorkerState = worker_framework::WorkerState;
using LocalState = worker_framework::LocalState;
using ZkState = worker_framework::ZkState;

std::unique_ptr<WorkerState> LogReplicatorCreator::createState(const std::string &stateFile) {
    if (autil::StringUtil::startsWith(stateFile, "zfs://")) {
        return std::unique_ptr<WorkerState>(ZkState::create(stateFile));
    } else {
        return std::make_unique<LocalState>(stateFile);
    }
}

} // namespace suez
