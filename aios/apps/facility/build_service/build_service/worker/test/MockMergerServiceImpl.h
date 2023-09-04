#ifndef ISEARCH_BS_MOCKMERGERSERVICEIMPL_H
#define ISEARCH_BS_MOCKMERGERSERVICEIMPL_H

#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/worker/MergerServiceImpl.h"
#include "build_service/worker/ServiceWorker.h"
#include "indexlib/util/EpochIdUtil.h"
namespace build_service { namespace worker {

class MockMergerServiceImpl : public MergerServiceImpl
{
public:
    MockMergerServiceImpl(const proto::PartitionId& pid, const std::string& zkRoot = "",
                          const std::string& appZkRoot = "", const std::string& adminServiceName = "")
        : MergerServiceImpl(pid, NULL, proto::LongAddress(), zkRoot, appZkRoot, adminServiceName,
                            indexlib::util::EpochIdUtil::TEST_GenerateEpochId())
    {
        ON_CALL(*this, getServiceConfig(_, _))
            .WillByDefault(Invoke(std::bind(&MockMergerServiceImpl::doGetServiceConfig, this, std::placeholders::_1,
                                            std::placeholders::_2)));
    }
    ~MockMergerServiceImpl() {}

private:
    bool doGetServiceConfig(config::ResourceReader& resourceReader, config::BuildServiceConfig& serviceConfig)
    {
        return MergerServiceImpl::getServiceConfig(resourceReader, serviceConfig);
    }

protected:
    MOCK_METHOD0(createMergeTask, task_base::MergeTask*());
    MOCK_METHOD2(getServiceConfig, bool(config::ResourceReader&, config::BuildServiceConfig&));
    MOCK_METHOD4(fillIndexInfo, bool(const std::string&, const proto::Range&, const std::string&, proto::IndexInfo*));
};

typedef ::testing::StrictMock<MockMergerServiceImpl> StrictMockMergerServiceImpl;
typedef ::testing::NiceMock<MockMergerServiceImpl> NiceMockMergerServiceImpl;

}} // namespace build_service::worker

#endif // ISEARCH_BS_MOCKMERGERSERVICEIMPL_H
