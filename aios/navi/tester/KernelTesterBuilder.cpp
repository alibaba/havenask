#include "navi/tester/KernelTesterBuilder.h"

#include "autil/EnvUtil.h"
#include "autil/legacy/jsonizable.h"
#include "navi/config/NaviConfig.h"
#include "navi/config_loader/Config.h"
#include "navi/tester/TestKernelConfigContext.h"
#include "navi/tester/TesterDef.h"
#include "navi/resource/MemoryPoolR.h"

using namespace autil::legacy;

namespace navi {

KernelTesterBuilder::KernelTesterBuilder(TestMode testMode) : _testMode(testMode) {
    snapshotResourceConfig(MemoryPoolR::RESOURCE_ID,
                           R"json({"use_asan_pool" : true})json");
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

KernelTesterBuilder &KernelTesterBuilder::snapshotResourceConfig(
        const std::string &resourceName,
        const std::string &configStr)
{
    _info.snapshotResourceConfig[resourceName] = configStr;
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::resource(const ResourceMap &resourceMap) {
    _info.resources->addResource(resourceMap);
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

KernelTesterBuilder &KernelTesterBuilder::enableKernel(const std::string &kernelNameRegex) {
    _info.kernelNameRegexVec.push_back(kernelNameRegex);
    return *this;
}

KernelTesterBuilder &KernelTesterBuilder::namedData(const std::string &name, DataPtr data) {
    NamedData namedData;
    namedData.name = name;
    namedData.data = std::move(data);
    _info.namedDataVec.emplace_back(std::move(namedData));
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
    bizConfig.partIds.emplace(0);
    bizConfig.kernels.insert("^" + _info.kernelName + "$");
    bizConfig.kernels.insert(NAVI_TESTER_CONTROL_KERNEL);
    bizConfig.kernels.insert(NAVI_TESTER_INPUT_KERNEL);
    bizConfig.kernels.insert(NAVI_TESTER_OUTPUT_KERNEL);
    for (const auto &name : _info.kernelNameRegexVec) {
        bizConfig.kernels.insert(name);
    }
    if (!fillConfig(config)) {
        return false;
    }
    return NaviConfig::prettyDumpToStr(config, configStr);
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
    for (const auto &pair : _info.snapshotResourceConfig) {
        const auto &name = pair.first;
        const auto &configStr = pair.second;
        NaviRegistryConfig registryConfig;
        if (!registryConfig.loadConfig(name, configStr)) {
            NAVI_KERNEL_LOG(ERROR, "create built-in resource [%s] registry failed",
                            name.c_str());
            return false;
        }
        config.snapshotResourceConfigVec.emplace_back(
            std::move(registryConfig));
    }
    return true;
}

bool KernelTesterBuilder::initNavi() {
    if (_navi) {
        return true;
    }
    NaviPtr navi(new Navi());
    navi->setTestMode(_testMode);
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
        navi.reset();
        NAVI_KERNEL_LOG(DEBUG, "navi init failed");
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
    const std::string naviPythonPath = "./aios/navi/config_loader/python";
    return Config::load(naviPythonPath, configLoader, configPath, loadParam,
                        jsonConfig);
}

}
