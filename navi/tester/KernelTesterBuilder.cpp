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
#include "navi/tester/KernelTesterBuilder.h"

#include "autil/EnvUtil.h"
#include "autil/legacy/jsonizable.h"
#include "navi/config/NaviConfig.h"
#include "navi/config_loader/Config.h"
#include "navi/tester/TestKernelConfigContext.h"
#include "navi/tester/TesterDef.h"

using namespace autil::legacy;

namespace navi {

KernelTesterBuilder::KernelTesterBuilder() {
}

KernelTesterBuilder::~KernelTesterBuilder() {
}

KernelTesterBuilder &KernelTesterBuilder::kernel(const std::string &kernelName){
    _info.kernelName = kernelName;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::input(const std::string &portName) {
    _info.inputs.insert(portName);
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::output(const std::string &portName) {
    _info.outputs.insert(portName);
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::attrs(const std::string &attrs) {
    _info.attrs = attrs;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::attrsFromMap(
        const std::map<std::string, std::string> &attrs)
{
    _info.attrs = FastToJsonString(attrs);
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::binaryAttrsFromMap(
        const std::map<std::string, std::string> &binaryAttrs)
{
    _info.binaryAttrs = binaryAttrs;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::integerAttrsFromMap(
        const std::map<std::string, int64_t> &integerAttrs)
{
    _info.integerAttrs = integerAttrs;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::skipConfig() {
    _info.skipConfig = true;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::skipInit() {
    _info.skipInit = true;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::deleteKernelFinish() {
    _info.skipDeleteKernel = false;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::kernelConfig(const std::string &configStr) {
    _info.configs = configStr;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::resourceConfig(
        const std::string &resourceName,
        const std::string &configStr)
{
    _info.resourceConfigs[resourceName] = configStr;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::buildinResourceConfig(
        const std::string &resourceName,
        const std::string &configStr)
{
    _info.buildinResourceConfigs[resourceName] = configStr;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::initResource(const std::string &resource) {
    _info.initResources.insert(resource);
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::resource(const ResourcePtr &resource) {
    _info.resources->addResource(resource);
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::logLevel(const std::string &level) {
    _info.logLevel = level;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::threadNum(size_t threadNum) {
    _info.threadNum = threadNum;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::module(const std::string &modulePath) {
    _info.modulePaths.push_back(modulePath);
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::configPath(const std::string &configPath) {
    _info.configPath = configPath;
    return *this;
}

bool KernelTesterBuilder::getNaviTestConfig(std::string &configStr) const {
    NaviConfig config;
    config.modules = _info.modulePaths;
    config.engineConfig.builtinTaskQueue.threadNum = _info.threadNum;
    config.engineConfig.builtinTaskQueue.queueSize = 1000;
    config.engineConfig.disablePerf = _info.disablePerf;
    config.engineConfig.disableSymbolTable = _info.disableSymbolTable;
    auto &bizConfig = config.bizMap[NAVI_TESTER_BIZ];
    bizConfig.partCount = 1;
    bizConfig.partIds.push_back(0);
    bizConfig.initResources = _info.initResources;
    if (!fillConfig(config)) {
        return false;
    }
    return config.dumpToStr(configStr);
}

bool KernelTesterBuilder::fillConfig(NaviConfig &config) const
{
    auto &bizConfig = config.bizMap[NAVI_TESTER_BIZ];
    if (!_info.configs.empty()) {
        const auto &name = _info.kernelName;
        const auto &configStr = _info.configs;
        NaviRegistryConfig registryConfig;
        if (!registryConfig.loadConfig(name, configStr)) {
            NAVI_KERNEL_LOG(ERROR, "create kernel [%s] config registry failed",
                            name.c_str());
            return false;
        }
        bizConfig.kernelConfigVec.push_back(registryConfig);
    }
    for (const auto &pair : _info.resourceConfigs) {
        const auto &name = pair.first;
        const auto &configStr = pair.second;
        NaviRegistryConfig registryConfig;
        if (!registryConfig.loadConfig(name, configStr)) {
            NAVI_KERNEL_LOG(ERROR, "create biz resource [%s] config registry failed",
                            name.c_str());
            return false;
        }
        bizConfig.resourceConfigVec.emplace_back(std::move(registryConfig));
    }
    for (const auto &pair : _info.buildinResourceConfigs) {
        const auto &name = pair.first;
        const auto &configStr = pair.second;
        NaviRegistryConfig registryConfig;
        if (!registryConfig.loadConfig(name, configStr)) {
            NAVI_KERNEL_LOG(ERROR, "create built-in resource [%s] registry failed",
                            name.c_str());
            return false;
        }
        config.buildinResourceConfigVec.emplace_back(std::move(registryConfig));
    }
    return true;
}

bool KernelTesterBuilder::initNavi() {
    if (_navi) {
        return true;
    }
    NaviPtr navi(new Navi());
    navi->setTestMode();
    {
        if (!navi->init("", nullptr)) {
            return false;
        }
    }
    std::string configStr;
    if (!getNaviTestConfig(configStr)) {
        return false;
    }
    NAVI_KERNEL_LOG(DEBUG, "navi config [%s]", configStr.c_str());
    ResourceMap resources;
    resources.addResource(*_info.resources);
    if (!navi->update(configStr, resources)) {
        return false;
    }
    _navi = navi;
    return true;
}

std::string KernelTesterBuilder::getLogPrefix(const std::string &testerName) const {
    NaviObjectLogger logger;
    logger.addPrefix("tester [%s] kernel [%s]", testerName.c_str(),
                     _info.kernelName.c_str());
    return logger.prefix;
}

std::unique_ptr<NaviLoggerProvider> KernelTesterBuilder::getLogProvider(
        const std::string &testerName) const
{
    auto prefix = getLogPrefix(testerName);
    std::unique_ptr<NaviLoggerProvider> provider;
    if (!NAVI_TLS_LOGGER) {
        provider.reset(new NaviLoggerProvider(_info.logLevel, prefix));
    }
    return provider;
}

KernelTesterPtr KernelTesterBuilder::build(const std::string &testerName) {
    auto provider = getLogProvider(testerName);
    if (!initNavi()) {
        return nullptr;
    }
    auto tester = new KernelTester(getLogPrefix(testerName), _navi, _info);
    tester->init();
    NAVI_KERNEL_LOG(DEBUG, "create tester [%p] success", tester);
    return KernelTesterPtr(tester);
}

ResourceMapPtr KernelTesterBuilder::buildResource(
    const std::set<std::string> &resources)
{
    auto provider = getLogProvider("");
    if (!initNavi()) {
        return nullptr;
    }
    ResourceMapPtr resourceMap(new ResourceMap());
    if (!_navi->createResource(NAVI_TESTER_BIZ, 1, 0, resources, *resourceMap)) {
        return nullptr;
    }
    return resourceMap;
}

KernelConfigContextPtr KernelTesterBuilder::buildConfigContext() {
    return TestKernelConfigContext::buildConfigContext(_info);
}

const std::string &KernelTesterBuilder::getTesterBizName() {
    return NAVI_TESTER_BIZ;
}

bool KernelTesterBuilder::loadPythonConfig(const std::string &configLoader,
                                           const std::string &configPath,
                                           const std::string &loadParam,
                                           std::string &jsonConfig)
{
    const std::string naviPythonPath = "./navi/config_loader/python";
    return Config::load(naviPythonPath, configLoader, configPath, loadParam,
                        jsonConfig);
}

}
