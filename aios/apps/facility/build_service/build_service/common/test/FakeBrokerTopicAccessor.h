#pragma once

#include "build_service/common/BrokerTopicAccessor.h"
#include "build_service/common/test/FakeBrokerTopicKeeper.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "fslib/util/FileUtil.h"

namespace build_service { namespace common {

class FakeBrokerTopicAccessor : public BrokerTopicAccessor
{
public:
    FakeBrokerTopicAccessor(proto::BuildId buildId, const std::string& brokerTopicZkPath)
        : BrokerTopicAccessor(buildId)
        , _brokerTopicZkPath(brokerTopicZkPath)
    {
    }
    ~FakeBrokerTopicAccessor() {}

private:
    FakeBrokerTopicAccessor(const FakeBrokerTopicAccessor&);
    FakeBrokerTopicAccessor& operator=(const FakeBrokerTopicAccessor&);

public:
    bool checkBrokerTopicExist(const std::string& topicName, bool expectedEnableVersionControl = false)
    {
        bool exist;
        std::string filePath = fslib::util::FileUtil::joinFilePath(_brokerTopicZkPath, topicName);
        if (!fslib::util::FileUtil::isExist(filePath, exist)) {
            assert(false);
            return false;
        }
        if (!exist) {
            return false;
        }
        std::string paramStr;
        if (!fslib::util::FileUtil::readFile(filePath, paramStr)) {
            assert(false);
            return false;
        }
        KeyValueMap param;
        autil::legacy::FromJsonString(param, paramStr);
        return (param["need_version_control"] == "true") == expectedEnableVersionControl;
    }

protected:
    common::BrokerTopicKeeperPtr createBrokerTopicKeeper() override
    {
        common::BrokerTopicKeeperPtr brokerTopic(new FakeBrokerTopicKeeper(_brokerTopicZkPath));
        return brokerTopic;
    }

private:
    std::string _brokerTopicZkPath;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeBrokerTopicAccessor);

}} // namespace build_service::common
