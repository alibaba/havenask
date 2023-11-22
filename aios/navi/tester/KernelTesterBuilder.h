#ifndef NAVI_KERNELTESTERBUILDER_H
#define NAVI_KERNELTESTERBUILDER_H

#include "navi/common.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/tester/KernelTester.h"

namespace navi {

class NaviBizConfig;

class KernelTesterBuilder
{
public:
    KernelTesterBuilder(TestMode testMode = TM_KERNEL_TEST);
    ~KernelTesterBuilder();
private:
    KernelTesterBuilder(const KernelTesterBuilder &);
    KernelTesterBuilder &operator=(const KernelTesterBuilder &);
public:
    KernelTesterBuilder &kernel(const std::string &kernelName);
    KernelTesterBuilder &input(const std::string &portName);
    KernelTesterBuilder &output(const std::string &portName);
    KernelTesterBuilder &attrs(const std::string &attrs);
    KernelTesterBuilder &attrsFromMap(const std::map<std::string, std::string> &attrs);
    KernelTesterBuilder &binaryAttrsFromMap(const std::map<std::string, std::string> &binaryAttrs);
    KernelTesterBuilder &integerAttrsFromMap(const std::map<std::string, int64_t> &integerAttrs);
    KernelTesterBuilder &skipConfig();
    KernelTesterBuilder &skipInit();
    KernelTesterBuilder &deleteKernelFinish();
    KernelTesterBuilder &kernelConfig(const std::string &configStr);
    KernelTesterBuilder &resourceConfig(const std::string &resourceName,
                                        const std::string &configStr);
    KernelTesterBuilder &snapshotResourceConfig(const std::string &resourceName,
                                                const std::string &configStr);
    KernelTesterBuilder &resource(const ResourceMap &resourceMap);
    KernelTesterBuilder &resource(const ResourcePtr &resource);
    KernelTesterBuilder &module(const std::string &modulePath);
    KernelTesterBuilder &configPath(const std::string &configPath);
    KernelTesterBuilder &logLevel(const std::string &level);
    KernelTesterBuilder &threadNum(size_t threadNum);
    KernelTesterBuilder &enableKernel(const std::string &kernelNameRegex);
    KernelTesterBuilder &namedData(const std::string &name, DataPtr data);
    KernelTesterPtr build(const std::string &testerName = "");
    ResourceMapPtr buildResource(const std::set<std::string> &resources);
    KernelConfigContextPtr buildConfigContext();
    const std::string &getTesterBizName();

public:
    static bool loadPythonConfig(const std::string &configLoader,
                                 const std::string &configPath,
                                 const std::string &loadParam,
                                 std::string &jsonConfig);
private:
    std::unique_ptr<NaviLoggerProvider> getLogProvider(
            const std::string &testerName) const;
    std::string getLogPrefix(const std::string &testerName) const;
    bool initNavi();
    bool getNaviTestConfig(std::string &configStr) const;
    bool fillConfig(NaviConfig &config) const;
private:
    NaviPtr _navi;
    KernelTestInfo _info;
    TestMode _testMode;
};

}

#endif //NAVI_KERNELTESTERBUILDER_H
