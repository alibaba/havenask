#ifndef ISEARCH_BS_BUILDSERVICECONFIG_H
#define ISEARCH_BS_BUILDSERVICECONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/CounterConfig.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class BuildServiceConfig : public autil::legacy::Jsonizable
{
public:
    BuildServiceConfig();
    ~BuildServiceConfig();
    BuildServiceConfig(const BuildServiceConfig&);
public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
public:
    bool operator == (const BuildServiceConfig &other) const;
    bool validate() const;
    std::string getApplicationId() const;
    std::string getMessageIndexName() const;
    std::string getBuilderIndexRoot(bool isFullBuilder) const;
    std::string getIndexRoot() const;
public:
    uint16_t amonitorPort;
    std::string userName;
    std::string serviceName;
    std::string swiftRoot;
    std::string hippoRoot;
    std::string zkRoot;
    std::string heartbeatType;
    CounterConfig counterConfig;
private:
    std::string _fullBuilderTmpRoot;
    std::string _indexRoot;
private:
    static const uint16_t DEFAULT_AMONITOR_PORT = 10086;
    static const std::string DEFAULT_MESSAGE_INDEX_NAME;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildServiceConfig);

}
}

#endif //ISEARCH_BS_BUILDSERVICECONFIG_H
