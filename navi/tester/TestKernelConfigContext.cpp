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
#include "navi/tester/TestKernelConfigContext.h"
#include "navi/config/NaviConfig.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/tester/KernelTester.h"

namespace navi {

TestKernelConfigContext::TestKernelConfigContext(const std::string &configPath)
    : KernelConfigContext(_configPath, nullptr)
    , _configPath(configPath)
    , _testNodeDef(nullptr)
{
}

TestKernelConfigContext::~TestKernelConfigContext() {
    DELETE_AND_SET_NULL(_testNodeDef);
}

bool TestKernelConfigContext::init(const KernelTestInfo &info) {
    if (!NaviConfig::parseToDocument(info.attrs, _jsonAttrsDocument)) {
        NAVI_KERNEL_LOG(ERROR, "invalid attribute");
        return false;
    }
    if (!NaviConfig::parseToDocument(info.configs, _jsonConfigDocument)) {
        NAVI_KERNEL_LOG(ERROR, "invalid config");
        return false;
    }
    _jsonAttrs = &_jsonAttrsDocument;
    _jsonConfig = &_jsonConfigDocument;
    if (!info.binaryAttrs.empty()) {
        initBinaryAttrs(info.binaryAttrs);
    }
    if (!info.integerAttrs.empty()) {
        initIntegerAttrs(info.integerAttrs);
    }
    _nodeDef = _testNodeDef;
    return true;
}

void TestKernelConfigContext::initBinaryAttrs(
    const std::map<std::string, std::string> &binaryAttrs)
{
    auto nodeDef = getNodeDef();
    auto pbBinaryAttrs = nodeDef->mutable_binary_attrs();
    for (const auto &pair : binaryAttrs) {
        (*pbBinaryAttrs)[pair.first] = pair.second;
    }
}

void TestKernelConfigContext::initIntegerAttrs(
    const std::map<std::string, int64_t> &integerAttrs)
{
    auto nodeDef = getNodeDef();
    auto pbIntegerAttrs = nodeDef->mutable_integer_attrs();
    for (const auto &pair : integerAttrs) {
        (*pbIntegerAttrs)[pair.first] = pair.second;
    }
}

NodeDef *TestKernelConfigContext::getNodeDef() {
    if (_testNodeDef) {
        return _testNodeDef;
    }
    _testNodeDef = new NodeDef();
    return _testNodeDef;
}

KernelConfigContextPtr TestKernelConfigContext::buildConfigContext(
        const KernelTestInfo &info)
{
    TestKernelConfigContextPtr ctx(new TestKernelConfigContext(info.configPath));
    if (!ctx->init(info)) {
        NAVI_KERNEL_LOG(ERROR, "build KernelConfigContext for test failed");
        return nullptr;
    }
    return std::dynamic_pointer_cast<KernelConfigContext>(ctx);
}

}

