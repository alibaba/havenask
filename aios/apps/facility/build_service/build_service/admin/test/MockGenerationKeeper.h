#ifndef ISEARCH_BS_MOCKGENERATIONKEEPER_H
#define ISEARCH_BS_MOCKGENERATIONKEEPER_H

#include <atomic>

#include "build_service/admin/GenerationKeeper.h"
#include "build_service/common_define.h"
#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class MockGenerationKeeper : public GenerationKeeper
{
public:
    MockGenerationKeeper(const proto::BuildId& buildId, const std::string& zkRoot, const std::string& adminServiceName,
                         const std::string& amonitorPort, cm_basic::ZkWrapper* zkWrapper,
                         const ProhibitedIpCollectorPtr& prohibitIpCollector, bool useFakeGenerationTask = false,
                         const std::shared_ptr<common::RpcChannelManager>& catalogRpcChannelManager = nullptr,
                         const std::shared_ptr<CatalogPartitionIdentifier>& catalogPartitionIdentifier = nullptr,
                         const std::vector<hippo::PackageInfo>& specifiedWorkerPkgList = {})
        : GenerationKeeper(Param {buildId, zkRoot, adminServiceName, amonitorPort, zkWrapper, prohibitIpCollector,
                                  nullptr, catalogRpcChannelManager, catalogPartitionIdentifier,
                                  specifiedWorkerPkgList})
        , _useFakeGenerationTask(useFakeGenerationTask)
        , _fakeIndexRoot(zkRoot)
    {
        ON_CALL(*this, needAutoStop()).WillByDefault(Invoke(std::bind(&MockGenerationKeeper::doNeedAutoStop, this)));
        ON_CALL(*this, generateRolePlan(_))
            .WillByDefault(Invoke(std::bind(&MockGenerationKeeper::doGenerateRolePlan, this, std::placeholders::_1)));
    }
    ~MockGenerationKeeper();

public:
    MOCK_METHOD0(needAutoStop, bool());
    bool doNeedAutoStop() { return GenerationKeeper::needAutoStop(); }
    GenerationTaskBase* createGenerationTask(const proto::BuildId& buildId, bool jobMode,
                                             const std::string& buildMode) override;
    GenerationTaskBase* innerRecoverGenerationTask(const std::string& status) override;
    void run(int64_t times)
    {
        for (int64_t i = 0; i < times; i++) {
            workThread();
        }
    }
    void mockSuspend(const std::string& clusterName);
    void setFakeIndexRoot(const std::string& fakeIndexRoot) { _fakeIndexRoot = fakeIndexRoot; }

    MOCK_METHOD1(generateRolePlan, GenerationRolePlanPtr(AgentSimpleMasterScheduler*));
    GenerationRolePlanPtr doGenerateRolePlan(AgentSimpleMasterScheduler* scheduler)
    {
        return GenerationKeeper::generateRolePlan(scheduler);
    }

public:
    bool _useFakeGenerationTask;
    std::string _fakeIndexRoot;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MockGenerationKeeper);

}} // namespace build_service::admin

#endif // ISEARCH_BS_MOCKGENERATIONKEEPER_H
