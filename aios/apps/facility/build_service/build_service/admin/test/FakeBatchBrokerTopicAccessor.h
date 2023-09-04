#ifndef ISEARCH_BS_FAKEBATCHBROKERTOPICACCESSOR_H
#define ISEARCH_BS_FAKEBATCHBROKERTOPICACCESSOR_H

#include "build_service/admin/BatchBrokerTopicAccessor.h"
#include "build_service/admin/test/FakeSwiftAdminAdapter.h"
#include "build_service/common/test/FakeBrokerTopicKeeper.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "fslib/util/FileUtil.h"

namespace build_service { namespace admin {

class FakeBatchBrokerTopicAccessor : public BatchBrokerTopicAccessor
{
public:
    FakeBatchBrokerTopicAccessor(proto::BuildId buildId, const std::string& brokerTopicZkPath,
                                 size_t maxBatchSize = BatchBrokerTopicAccessor::DEFAULT_MAX_BATCH_SIZE)
        : BatchBrokerTopicAccessor(buildId, maxBatchSize)
        , _brokerTopicZkPath(brokerTopicZkPath)
    {
        _fakeSwiftAdminAdapter.reset(new swift::client::FakeSwiftAdminAdapter(_brokerTopicZkPath));
    }

    ~FakeBatchBrokerTopicAccessor() {}

private:
    FakeBatchBrokerTopicAccessor(const FakeBatchBrokerTopicAccessor&);
    FakeBatchBrokerTopicAccessor& operator=(const FakeBatchBrokerTopicAccessor&);

public:
    bool checkBrokerTopicExist(const std::string& topicName)
    {
        bool exist;
        if (!fslib::util::FileUtil::isExist(fslib::util::FileUtil::joinFilePath(_brokerTopicZkPath, topicName),
                                            exist)) {
            assert(false);
            return false;
        }
        return exist;
    }

    void setResponse(swift::protocol::ErrorCode ec, const std::string& errorMsg)
    {
        _fakeSwiftAdminAdapter->setResponse(ec, errorMsg);
    }

    const std::vector<std::string>& getLatestCreateTopics() const
    {
        return _fakeSwiftAdminAdapter->getLatestCreateTopics();
    }

    const std::vector<std::string>& getLatestDeleteTopics() const
    {
        return _fakeSwiftAdminAdapter->getLatestDeleteTopics();
    }

protected:
    common::BrokerTopicKeeperPtr createBrokerTopicKeeper() override
    {
        common::BrokerTopicKeeperPtr brokerTopic(new common::FakeBrokerTopicKeeper(_brokerTopicZkPath));
        return brokerTopic;
    }

    swift::network::SwiftAdminAdapterPtr createSwiftAdminAdapter(const std::string& zkPath) override
    {
        return _fakeSwiftAdminAdapter;
    }

private:
    std::string _brokerTopicZkPath;
    swift::client::FakeSwiftAdminAdapterPtr _fakeSwiftAdminAdapter;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeBatchBrokerTopicAccessor);

}} // namespace build_service::admin

#endif // ISEARCH_BS_FAKEBATCHBROKERTOPICACCESSOR_H
