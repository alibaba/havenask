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
#include "build_service/util/SwiftClientCreator.h"

#include "autil/StringUtil.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "fslib/util/FileUtil.h"
#include "swift/client/SwiftClientConfig.h"

using namespace std;
using namespace autil;
using namespace swift::client;
using namespace build_service::proto;

namespace build_service { namespace util {
BS_LOG_SETUP(util, SwiftClientCreator);

SwiftClientCreator::SwiftClientCreator() { setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME); }
SwiftClientCreator::SwiftClientCreator(const kmonitor::MetricsReporterPtr& metricsReporter)
    : _metricsReporter(metricsReporter)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}

SwiftClientCreator::~SwiftClientCreator()
{
    map<string, SwiftClient*>::iterator iter = _swiftClientMap.begin();
    for (; iter != _swiftClientMap.end(); iter++) {
        DELETE_AND_SET_NULL(iter->second);
    }
    _swiftClientMap.clear();
}

SwiftClient* SwiftClientCreator::createSwiftClient(const string& zfsRootPath, const string& swiftClientConfig)
{
    ScopedLock lock(_lock);
    string key = zfsRootPath + ";" + swiftClientConfig;
    map<string, SwiftClient*>::iterator iter = _swiftClientMap.find(key);
    if (iter != _swiftClientMap.end()) {
        assert(iter->second);
        return iter->second;
    }
    SwiftClient* client = doCreateSwiftClient(zfsRootPath, swiftClientConfig);
    if (client == NULL) {
        return NULL;
    }
    _swiftClientMap.insert(make_pair(key, client));
    return client;
}

string SwiftClientCreator::getInitConfigStr(const string& zfsRootPath, const string& swiftClientConfig)
{
    auto zkVec = autil::StringUtil::split(zfsRootPath, swift::client::SwiftClientConfig::CLIENT_CONFIG_MULTI_SEPERATOR);
    stringstream configStr;
    for (auto i = 0; i < zkVec.size(); ++i) {
        configStr << swift::client::CLIENT_CONFIG_ZK_PATH << "=" << zkVec[i];
        if (!swiftClientConfig.empty()) {
            configStr << swift::client::CLIENT_CONFIG_SEPERATOR << swiftClientConfig;
        }
        if (i + 1 != zkVec.size()) {
            configStr << swift::client::SwiftClientConfig::CLIENT_CONFIG_MULTI_SEPERATOR;
        }
    }
    return configStr.str();
}

SwiftClient* SwiftClientCreator::doCreateSwiftClient(const std::string& zfsRootPath, const string& swiftClientConfig)
{
    SwiftClient* client = new SwiftClient(_metricsReporter);
    string configStr = getInitConfigStr(zfsRootPath, swiftClientConfig);
    BS_LOG(INFO, "create SwiftClient, configStr is [%s].", configStr.c_str());
    swift::protocol::ErrorCode errorCode = client->initByConfigStr(configStr);

    if (errorCode == swift::protocol::ERROR_NONE) {
        return client;
    }

    if (errorCode == swift::protocol::ERROR_CLIENT_INIT_INVALID_PARAMETERS) {
        string errorMsg =
            string("connect to swift: ") + zfsRootPath + " failed." + ", invalid swift parameters:" + configStr;
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
    } else {
        string errorMsg = string("connect to swift: ") + zfsRootPath + " failed.";
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CONNECT_SWIFT, errorMsg, BS_STOP);
    }

    delete client;
    return NULL;
}
}} // namespace build_service::util
