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
#include "suez/drc/LogReplicator.h"

#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "build_service/util/SwiftClientCreator.h"
#include "indexlib/framework/ITablet.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/drc/LogReplicationPipeline.h"
#include "suez/drc/LogReplicatorCreator.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez.drc, LogReplicator);

LogReplicator::LogReplicator(const DrcConfig &config,
                             const build_service::util::SwiftClientCreatorPtr &swiftClientCreator,
                             std::shared_ptr<indexlibv2::framework::ITablet> index,
                             const kmonitor::MetricsReporterPtr &reporter)
    : _config(config)
    , _swiftClientCreator(swiftClientCreator)
    , _index(std::move(index))
    , _metricsReporter(reporter)
    , _running(false) {}

LogReplicator::~LogReplicator() {}

bool LogReplicator::start() {
    auto loopFn = [this]() { workLoop(); };

    _workThread =
        autil::LoopThread::createLoopThread(std::move(loopFn), _config.getLogLoopIntervalMs() * 1000, "LogRep");
    if (!_workThread) {
        return false;
    }

    _running = true;
    return true;
}

void LogReplicator::stop() {
    _running = false;
    {
        autil::ScopedReadLock lock(_lock);
        for (const auto &it : _pipelines) {
            if (it.second) {
                it.second->stop();
            }
        }
    }
    if (_workThread) {
        _workThread->stop();
        _workThread.reset();
    }
    _pipelines.clear();
}

void LogReplicator::workLoop() {
    if (!_running) {
        return;
    }

    maybeCreatePipelines();

    autil::ScopedReadLock lock(_lock);
    // TODO: maybe use a thread pool to schedule each pipeline
    for (const auto &it : _pipelines) {
        it.second->run();
    }
}

void LogReplicator::maybeCreatePipelines() {
    autil::ScopedWriteLock lock(_lock);
    const auto &sinkConfigs = _config.getSinkConfigs();
    if (_config.shareMode() || sinkConfigs.size() == 1) {
        if (_pipelines.size() == 1) {
            return;
        }
        auto pipeline = createPipeline(_config);
        if (!pipeline) {
            AUTIL_LOG(ERROR, "create shared pipeline failed");
            return;
        }
        _pipelines.emplace("share", std::move(pipeline));
    } else {
        AUTIL_LOG(INFO, "create pipelines for non-shared mode drc");
        for (const auto &sinkConfig : sinkConfigs) {
            if (_pipelines.count(sinkConfig.name)) {
                continue;
            }
            auto config = _config.deriveForSingleSink(sinkConfig);
            auto pipeline = createPipeline(config);
            if (!pipeline) {
                AUTIL_LOG(ERROR, "create pipeline for %s failed", sinkConfig.name.c_str());
                continue;
            }
            AUTIL_LOG(INFO, "create private pipeline for sink %s", sinkConfig.name.c_str());
            _pipelines.emplace(sinkConfig.name, std::move(pipeline));
        }
    }
}

std::shared_ptr<LogReplicationPipeline> LogReplicator::createPipeline(const DrcConfig &config) const {
    auto pipeline = LogReplicatorCreator::create(config, _swiftClientCreator, _index, _metricsReporter);
    if (pipeline && pipeline->start(config.getStartOffset())) {
        return pipeline;
    }
    return nullptr;
}

} // namespace suez
