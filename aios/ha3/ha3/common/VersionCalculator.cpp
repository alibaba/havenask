#include <ha3/common/VersionCalculator.h>
#include <ha3/version.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, VersionCalculator);

uint32_t VersionCalculator::calcVersion(const std::string &workerConfigVersion,
                                        const std::string &configDataVersion,
                                        const std::string &remoteConfigPath,
                                        const std::string &fullVersionStr)
{
    string delim(":");
    string versionStr;
    if (!workerConfigVersion.empty()) {
        versionStr = HA_BUILD_SIGNATURE + delim + workerConfigVersion;
    } else if (!configDataVersion.empty()) {
        versionStr = HA_BUILD_SIGNATURE + delim + fullVersionStr + delim + configDataVersion;
    } else {
        versionStr = HA_BUILD_SIGNATURE + delim + fullVersionStr + delim + remoteConfigPath;
    }
    uint32_t version = autil::HashAlgorithm::hashString(versionStr.c_str(),
            versionStr.size(), 0);
    HA3_LOG(INFO, "ha3 version str: [%s], version [%u]",
            versionStr.c_str(), version);
    return version;
}

END_HA3_NAMESPACE(common);
