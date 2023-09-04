#include "build_service/util/test/FakeSwiftClientCreator.h"

#include "build_service/util/test/FakeSwiftClient.h"

using namespace std;
using namespace swift::client;

namespace build_service { namespace util {
BS_LOG_SETUP(util, FakeSwiftClientCreator);

FakeSwiftClientCreator::FakeSwiftClientCreator(bool hasReader, bool hasWriter)
    : _hasReader(hasReader)
    , _hasWriter(hasWriter)
{
}

FakeSwiftClientCreator::~FakeSwiftClientCreator() {}

SwiftClient* FakeSwiftClientCreator::doCreateSwiftClient(const std::string& zfsRootPath,
                                                         const string& swiftClientConfig)
{
    SwiftClient* client = new FakeSwiftClient(_hasReader, _hasWriter);
    BS_LOG(INFO, "create FakeSwiftClient, _hasReader[%d], _hasWriter[%d].", int(_hasReader), int(_hasWriter));
    string configStr = getInitConfigStr(zfsRootPath, swiftClientConfig);
    BS_LOG(INFO, "create SwiftClient, configStr is [%s].", configStr.c_str());
    client->initByConfigStr(configStr);
    return client;
}

string FakeSwiftClientCreator::getInitConfigStr(const string& zfsRootPath, const string& swiftClientConfig)
{
    stringstream configStr;
    configStr << swift::client::CLIENT_CONFIG_ZK_PATH << "=" << zfsRootPath;
    if (!swiftClientConfig.empty()) {
        configStr << swift::client::CLIENT_CONFIG_SEPERATOR << swiftClientConfig;
    }
    return configStr.str();
}

}} // namespace build_service::util
