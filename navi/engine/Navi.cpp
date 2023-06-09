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
#include "navi/engine/Navi.h"
#include "navi/config/NaviConfig.h"
#include "navi/config_loader/Config.h"
#include "navi/distribute/NaviServerStreamCreator.h"
#include "navi/engine/NaviSnapshot.h"
#include "navi/engine/NaviMetricsGroup.h"
#include "navi/resource/MemoryPoolResource.h"
#include "navi/util/CommonUtil.h"
#include <fslib/fslib.h>
#include <multi_call/agent/GigAgent.h>
#include <multi_call/rpc/GigRpcServer.h>

namespace navi {

Navi::Navi()
    : Navi(nullptr)
{
}

Navi::Navi(const std::shared_ptr<CreatorManager> &creatorManager)
    : _testMode(false)
{
    initInstanceId();
}

Navi::~Navi() {
    stop();
}

bool Navi::init(const std::string &initConfigPath,
                multi_call::GigRpcServer *gigRpcServer)
{
    initDefaultLogger();
    NAVI_LOG(INFO, "start init with gigRpcServer [%p], config [%s]",
             gigRpcServer,
             initConfigPath.c_str());
    if (!initRpc(gigRpcServer)) {
        return false;
    }
    if (!initMetricsReportThread()) {
        return false;
    }
    if (!initMemoryPoolResource()) {
        return false;
    }
    _initConfigPath = initConfigPath;
    return true;
}

void Navi::initMetrics(kmonitor::MetricsReporter &reporter) {
    _metricsReporter = reporter.getSubReporter("", {});
}

bool Navi::initRpc(multi_call::GigRpcServer *gigRpcServer) {
    if (!gigRpcServer) {
        NAVI_LOG(INFO, "skip init rpc as gigRpcServer is nullptr.");
        return true;
    }
    _gigRpcServer = gigRpcServer;
    _streamCreator.reset(new NaviServerStreamCreator(_logger.logger, this));
    if (!_gigRpcServer->registerGrpcStreamService(
            std::dynamic_pointer_cast<multi_call::GigServerStreamCreator>(
                _streamCreator)))
    {
        NAVI_LOG(ERROR,
                 "register navi stream rpc failed, check whether the grpc server is initialized");
        return false;
    }
    auto gigAgent = _gigRpcServer->getAgent();
    assert(gigAgent);
    gigAgent->start(); // start agent to receive query
    return true;
}

bool Navi::update(const std::string &configLoader,
                  const std::string &configPath,
                  const std::string &loadParam,
                  const ResourceMap &rootResourceMap)
{
    std::string configStr;
    {
        initDefaultLogger();
        NaviLoggerScope scope(_logger);
        if (!Config::load(getNaviPythonPath(), configLoader, configPath,
                          loadParam, configStr))
        {
            NAVI_LOG(ERROR, "load navi config from python failed");
            return false;
        }
    }
    return update(configStr, rootResourceMap);
}

bool Navi::update(const NaviConfig &config,
                  const ResourceMap &rootResourceMap)
{
    std::string configStr;
    {
        initDefaultLogger();
        NaviLoggerScope scope(_logger);
        if (!config.dumpToStr(configStr)) {
            NAVI_LOG(ERROR, "dump navi config to str failed");
            return false;
        }
    }
    return update(configStr, rootResourceMap);
}

bool Navi::update(const std::string &configStr,
                  const ResourceMap &rootResourceMapIn)
{
    ResourceMap rootResourceMap;
    rootResourceMap.addResource(rootResourceMapIn);
    rootResourceMap.addResource(_memoryPoolResource);
    NaviConfigPtr config(new NaviConfig());
    {
        initDefaultLogger();
        NaviLoggerScope scope(_logger);
        if (!config->loadConfig(configStr)) {
            return false;
        }
    }
    NaviSnapshotPtr snapshot(new NaviSnapshot());
    snapshot->setTestMode(_testMode);
    if (_metricsReporter) {
        snapshot->initMetrics(*_metricsReporter);
    }
    if (!snapshot->init(_initConfigPath, _instanceId, _creatorManager,
                        getSnapshot(), config, rootResourceMap))
    {
        snapshot->stop();
        return false;
    }
    updateSnapshot(snapshot);
    return true;
}

void Navi::updateSnapshot(const NaviSnapshotPtr &newSnapshot) {
    NAVI_LOG(INFO, "start update snapshot, new [%p]", newSnapshot.get());
    NaviSnapshotPtr oldSnapshot;
    {
        autil::ScopedWriteLock scope(_snapshotLock);
        oldSnapshot = _snapshot;
        _snapshot = newSnapshot;
        if (newSnapshot) {
            _logger.logger = newSnapshot->getLogger();
            _logger.prefix.clear();
            _logger.addPrefix("navi");
        }
    }
    if (oldSnapshot) {
        NaviLoggerScope scope(oldSnapshot->_logger);
        CommonUtil::waitUseCount(oldSnapshot, 1);
        oldSnapshot->stop();
    }
}

NaviSnapshotPtr Navi::getSnapshot() const {
    autil::ScopedReadLock scope(_snapshotLock);
    return _snapshot;
}

void Navi::initDefaultLogger() {
    auto oldSnapshot = getSnapshot();
    if (oldSnapshot) {
        _logger.logger = oldSnapshot->getLogger();
        _logger.object = this;
        return;
    }
    auto tlsLogger = NaviSnapshot::getTlsLogger();
    if (tlsLogger) {
        _logger.logger = tlsLogger;
    } else {
        NaviLogManagerPtr logManager(new NaviLogManager(_instanceId));
        logManager->init(LogConfig::getDefaultLogConfig()); // default config;
        _logger.logger = logManager->createLogger();
    }
    _logger.object = this;
    _logger.prefix.clear();
    _logger.addPrefix("navi snapshot before load config");
}

void Navi::stopSnapshot() {
    NAVI_LOG(INFO, "navi stop snapshot begin");
    NaviLoggerScope scope(_logger);
    updateSnapshot(nullptr);
}

void Navi::stop() {
    NAVI_LOG(INFO, "navi stop begin");
    NaviLoggerScope scope(_logger);
    if (_streamCreator) {
        _streamCreator->stop();
    }
    _metricsReportThread.reset();
    stopSnapshot();
    _creatorManager.reset();
    NAVI_LOG(INFO, "instance stopped");
}

NaviUserResultPtr Navi::runLocalGraph(GraphDef *graphDef,
                                      const RunGraphParams &params,
                                      const ResourceMap &resourceMap)
{
    return doRunGraph(graphDef, params, &resourceMap);
}

void Navi::runLocalGraphAsync(GraphDef *graphDef,
                              const RunGraphParams &params,
                              const ResourceMap &resourceMap,
                              NaviUserResultClosure *closure)
{
    return doRunGraphAsync(graphDef, params, &resourceMap, closure);
}

NaviUserResultPtr Navi::runGraph(GraphDef *graphDef,
                                 const RunGraphParams &params)
{
    return doRunGraph(graphDef, params, nullptr);
}

NaviUserResultPtr Navi::doRunGraph(GraphDef *graphDef,
                                   const RunGraphParams &params,
                                   const ResourceMap *resourceMap)
{
    auto snapshot = getSnapshot();
    if (!snapshot) {
        multi_call::freeProtoMessage(graphDef);
        NAVI_LOG(WARN, "snapshot not exists, run graph skipped");
        return nullptr;
    }
    return snapshot->runGraph(graphDef, params, resourceMap);
}

void Navi::doRunGraphAsync(GraphDef *graphDef,
                           const RunGraphParams &params,
                           const ResourceMap *resourceMap,
                           NaviUserResultClosure *closure)
{
    auto snapshot = getSnapshot();
    if (!snapshot) {
        multi_call::freeProtoMessage(graphDef);
        NAVI_LOG(WARN, "snapshot not exists, run graph skipped");
        closure->run(nullptr);
        return;
    }
    return snapshot->runGraphAsync(graphDef, params, resourceMap, closure);
}

std::shared_ptr<CreatorManager> Navi::getCreatorManager() const {
    auto snapshot = getSnapshot();
    if (!snapshot) {
        return nullptr;
    }
    return snapshot->getCreatorManager();
}

bool Navi::initMetricsReportThread() {
    _metricsReportThread = autil::LoopThread::createLoopThread(
            std::bind(&Navi::reportMetricsLoop, this),
            50 * 1000, "naviMetricsReport");
    if (!_metricsReportThread) {
        NAVI_KERNEL_LOG(ERROR, "Create metrics report thread fail!");
        return false;
    }
    return true;
}

bool Navi::initMemoryPoolResource() {
    NaviLoggerScope scope(_logger);
    auto resource = std::make_shared<MemoryPoolResource>();
    if (!resource->init(_metricsReporter)) {
        NAVI_KERNEL_LOG(ERROR, "Create worker level memory pool resource failed");
        return false;
    }
    _memoryPoolResource = std::move(resource);
    return true;
}

void Navi::initInstanceId() {
    _instanceId = 0;
    while (0 == _instanceId) {
        _instanceId = CommonUtil::random64();
    }
}

InstanceId Navi::getInstanceId() const {
    return _instanceId;
}

void Navi::setTestMode() {
    _testMode = true;
}

bool Navi::createResource(const std::string &bizName,
                          NaviPartId partCount,
                          NaviPartId partId,
                          const std::set<std::string> &resources,
                          ResourceMap &resourceMap) const
{
    resourceMap.addResource(_memoryPoolResource);
    auto snapshot = getSnapshot();
    if (!snapshot) {
        return false;
    }
    return snapshot->createResource(bizName, partCount, partId, resources,
                                    resourceMap);
}

std::string Navi::getNaviPythonPath() {
    return fslib::fs::FileSystem::joinFilePath(
        _initConfigPath, "usr/local/lib/python/site-packages/navi");
}

void Navi::reportMetricsLoop() {
    if (!_metricsReporter) {
        return;
    }
    auto snapshot = getSnapshot();
    if (!snapshot) {
        return;
    }
    snapshot->reportStat(*_metricsReporter);
}

}
