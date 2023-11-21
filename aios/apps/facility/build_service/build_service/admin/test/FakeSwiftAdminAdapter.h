#pragma once

#include <string>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift { namespace client {

class FakeSwiftAdminAdapter : public network::SwiftAdminAdapter
{
public:
    FakeSwiftAdminAdapter(const std::string& path);
    ~FakeSwiftAdminAdapter();

private:
    FakeSwiftAdminAdapter(const FakeSwiftAdminAdapter&);
    FakeSwiftAdminAdapter& operator=(const FakeSwiftAdminAdapter&);

public:
    void setResponse(protocol::ErrorCode ec, const std::string& errorMsg)
    {
        _ec = ec;
        _errorMsg = errorMsg;
    }

    protocol::ErrorCode deleteTopicBatch(protocol::TopicBatchDeletionRequest& request,
                                         protocol::TopicBatchDeletionResponse& response) override;

    protocol::ErrorCode createTopicBatch(protocol::TopicBatchCreationRequest& request,
                                         protocol::TopicBatchCreationResponse& response) override;

    const std::vector<std::string>& getLatestCreateTopics() const { return _latestCreateTopics; }

    const std::vector<std::string>& getLatestDeleteTopics() const { return _latestDeleteTopics; }

private:
    protocol::ErrorCode _ec;
    std::string _errorMsg;
    std::string _topicZkPath;
    std::vector<std::string> _latestCreateTopics;
    std::vector<std::string> _latestDeleteTopics;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeSwiftAdminAdapter);

}} // namespace swift::client
