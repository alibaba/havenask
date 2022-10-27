#include <swift/client/SwiftClientConfig.h>
#include "build_service/util/SwiftClientCreator.h"
#include "build_service/util/EnvUtil.h"
#include "build_service/util/FileUtil.h"
#include "build_service/common/BeeperCollectorDefine.h"

using namespace std;
using namespace autil;
SWIFT_USE_NAMESPACE(client);
using namespace build_service::proto;

namespace build_service {
namespace util {
BS_LOG_SETUP(util, SwiftClientCreator);

SwiftClientCreator::SwiftClientCreator() {
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}

SwiftClientCreator::~SwiftClientCreator() {
    map<string, SwiftClient*>::iterator iter = _swiftClientMap.begin();
    for(; iter != _swiftClientMap.end(); iter++)
    {
        DELETE_AND_SET_NULL(iter->second);
    }
    _swiftClientMap.clear();
}

SwiftClient * SwiftClientCreator::createSwiftClient(
        const string& zfsRootPath, const string& swiftClientConfig)
{
    ScopedLock lock(_lock);
    string key = zfsRootPath + ";" + swiftClientConfig;
    map<string, SwiftClient*>::iterator iter = _swiftClientMap.find(key);
    if (iter != _swiftClientMap.end())
    {
        assert(iter->second);
        return iter->second;
    }
    SwiftClient* client = doCreateSwiftClient(zfsRootPath, swiftClientConfig);
    if (client == NULL)
    {
        return NULL;
    }
    _swiftClientMap.insert(make_pair(key, client));
    return client;
}

string SwiftClientCreator::getInitConfigStr(const string &zfsRootPath, const string& swiftClientConfig)
{
    stringstream configStr;
    configStr << swift::client::CLIENT_CONFIG_ZK_PATH << "=" << zfsRootPath;
    if (!swiftClientConfig.empty()) {
        configStr << swift::client::CLIENT_CONFIG_SEPERATOR << swiftClientConfig;
    }
    return configStr.str();
}
    
SwiftClient * SwiftClientCreator::doCreateSwiftClient(const std::string &zfsRootPath,
        const string& swiftClientConfig)
{
    SwiftClient *client = new SwiftClient();
    string configStr = getInitConfigStr(zfsRootPath, swiftClientConfig);
    BS_LOG(INFO, "create SwiftClient, configStr is [%s].", configStr.c_str());
    SWIFT_NS(protocol)::ErrorCode errorCode = client->initByConfigStr(configStr);
    
    if (errorCode == SWIFT_NS(protocol)::ERROR_NONE) {
        return client;
    }
    
    if (errorCode == SWIFT_NS(protocol)::ERROR_CLIENT_INIT_INVALID_PARAMETERS)
    {
        string errorMsg = string("connect to swift: ") + zfsRootPath + " failed."
            + ", invalid swift parameters:" + configStr;
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
    }
    else
    {
        string errorMsg = string("connect to swift: ") + zfsRootPath + " failed.";
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CONNECT_SWIFT, errorMsg, BS_STOP);        
    }
    
    delete client;
    return NULL;
}
}
}
