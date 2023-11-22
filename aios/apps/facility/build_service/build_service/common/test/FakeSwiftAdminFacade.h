#pragma once

#include <string>

#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/common_define.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/util/Log.h"
#include "swift/network/SwiftAdminAdapter.h"

namespace build_service { namespace common {

class FakeSwiftAdminFacade : public SwiftAdminFacade
{
public:
    FakeSwiftAdminFacade(const std::string& brokerTopicZkPath)
        : _brokerTopicZkPath(brokerTopicZkPath)
        , _createTopicSuccess(true)
        , _deleteTopicSuccess(true)
    {
    }
    FakeSwiftAdminFacade(const FakeSwiftAdminFacade& other)
        : SwiftAdminFacade(other)
        , _brokerTopicZkPath(other._brokerTopicZkPath)
        , _createTopicSuccess(other._createTopicSuccess)
        , _deleteTopicSuccess(other._deleteTopicSuccess)
    {
    }
    ~FakeSwiftAdminFacade();

    swift::network::SwiftAdminAdapterPtr createSwiftAdminAdapter(const std::string& zkPath) override
    {
        if (!_adapter) {
            _adapter.reset(new swift::network::SwiftAdminAdapter("", swift::network::SwiftRpcChannelManagerPtr()));
        }
        return _adapter;
    }

    bool createTopic(const swift::network::SwiftAdminAdapterPtr& adapter, const std::string& topicName,
                     const config::SwiftTopicConfig& config, bool needVersionControl) override;
    bool deleteTopic(const swift::network::SwiftAdminAdapterPtr& adapter, const std::string& topicName) override;
    bool checkZkStatus(const std::string& nodePath, const std::string& result);
    void setDeleteTopicResult(bool success) { _deleteTopicSuccess = success; }

    void setCreateTopicResult(bool success) { _createTopicSuccess = success; }

private:
    std::string _brokerTopicZkPath;
    swift::network::SwiftAdminAdapterPtr _adapter;
    bool _createTopicSuccess;
    bool _deleteTopicSuccess;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeSwiftAdminFacade);

}} // namespace build_service::common
