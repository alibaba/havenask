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
    KernelTesterBuilder();
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
    KernelTesterBuilder &buildinResourceConfig(const std::string &resourceName,
                                               const std::string &configStr);
    KernelTesterBuilder &initResource(const std::string &resource);
    KernelTesterBuilder &resource(const ResourcePtr &resource);
    KernelTesterBuilder &module(const std::string &modulePath);
    KernelTesterBuilder &configPath(const std::string &configPath);
    KernelTesterBuilder &logLevel(const std::string &level);
    KernelTesterBuilder &threadNum(size_t threadNum);
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
};

}

#endif //NAVI_KERNELTESTERBUILDER_H
