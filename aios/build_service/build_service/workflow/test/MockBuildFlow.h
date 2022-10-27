#ifndef ISEARCH_BS_MOCKBUILDFLOW_H
#define ISEARCH_BS_MOCKBUILDFLOW_H

#include <deque>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/test/MockFlowFactory.h"
#include <indexlib/partition/index_partition.h>

namespace build_service {
namespace workflow {

class MockBuildFlow;
typedef ::testing::StrictMock<MockBuildFlow> StrictMockBuildFlow;
typedef ::testing::NiceMock<MockBuildFlow> NiceMockBuildFlow;

class MockBuildFlow : public BuildFlow
{
public:
    MockBuildFlow(const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPartition =
                  IE_NAMESPACE(partition)::IndexPartitionPtr(),
                  const BuildFlowThreadResource &threadResource = BuildFlowThreadResource())
        : BuildFlow(util::SwiftClientCreatorPtr(), indexPartition, threadResource)
    {
        ON_CALL(*this, createFlowFactory()).WillByDefault(
                Invoke(std::tr1::bind(&MockBuildFlow::createMockFactory)));
        ON_CALL(*this, isStarted()).WillByDefault(
                Invoke(std::tr1::bind(&MockBuildFlow::testIsStarted, this))); 
        ON_CALL(*this, isFinished()).WillByDefault(
                Invoke(std::tr1::bind(&MockBuildFlow::testIsFinished, this))); 
        ON_CALL(*this, getBuilder()).WillByDefault(
                Invoke(std::tr1::bind(&MockBuildFlow::doGetBuilder, this)));
        ON_CALL(*this, getProcessor()).WillByDefault(
                Invoke(std::tr1::bind(&MockBuildFlow::doGetProcessor, this)));
        ON_CALL(*this, getReader()).WillByDefault(
                Invoke(std::tr1::bind(&MockBuildFlow::doGetReader, this)));
        ON_CALL(*this, initRoleInitParam(_, _, _, _, _, _)).WillByDefault(
                Invoke(std::tr1::bind(&MockBuildFlow::doInitRoleInitParam, this,
                                std::tr1::placeholders::_1, std::tr1::placeholders::_2,
                                std::tr1::placeholders::_3, std::tr1::placeholders::_4,
                                std::tr1::placeholders::_5, std::tr1::placeholders::_6)));
        ON_CALL(*this, suspendReadAtTimestamp(_,_))
            .WillByDefault(Invoke(std::tr1::bind(&MockBuildFlow::doSuspendReadAtTimesamp,
                                                 this, std::tr1::placeholders::_1,
                                                 std::tr1::placeholders::_2)));
        ON_CALL(*this, startBuildFlow(_, _, _, _, _, _, _))
            .WillByDefault(Invoke(std::tr1::bind(&MockBuildFlow::doStartBuildFlow, this,
                                std::tr1::placeholders::_1, std::tr1::placeholders::_2,
                                std::tr1::placeholders::_3, std::tr1::placeholders::_4,
                                std::tr1::placeholders::_5, std::tr1::placeholders::_6,
                                                 std::tr1::placeholders::_7)));
    }
    ~MockBuildFlow() {}
private:
    MockBuildFlow(const MockBuildFlow &);
    MockBuildFlow& operator=(const MockBuildFlow &);
public:
    MOCK_CONST_METHOD0(isStarted, bool());
    MOCK_CONST_METHOD0(isFinished, bool());    
    MOCK_METHOD2(suspendReadAtTimestamp, bool(int64_t, reader::RawDocumentReader::ExceedTsAction));
    MOCK_CONST_METHOD0(getBuilder, builder::Builder*());
    MOCK_CONST_METHOD0(getProcessor, processor::Processor*());
    MOCK_CONST_METHOD0(getReader, reader::RawDocumentReader*());
    MOCK_CONST_METHOD0(createFlowFactory, BrokerFactory*());
    MOCK_METHOD6(initRoleInitParam, bool(const config::ResourceReaderPtr &,
                                         const KeyValueMap &, const proto::PartitionId &, WorkflowMode,
                                         IE_NAMESPACE(misc)::MetricProviderPtr, BrokerFactory::RoleInitParam&));
    MOCK_CONST_METHOD1(fillErrorInfos, void(std::deque<proto::ErrorInfo> &));
    MOCK_CONST_METHOD1(getLocator, bool(common::Locator &));
    MOCK_METHOD7(startBuildFlow, bool(const config::ResourceReaderPtr &,
                                      const KeyValueMap &,
                                      const proto::PartitionId &,
                                      BrokerFactory *,
                                      BuildFlow::BuildFlowMode,
                                      WorkflowMode,
                                      IE_NAMESPACE(misc)::MetricProviderPtr));
private:
    bool doInitRoleInitParam(const config::ResourceReaderPtr &resourceReader,
                             const KeyValueMap &kvMap,
                             const proto::PartitionId &partitionId,
                             WorkflowMode workflowMode,
                             IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
                             BrokerFactory::RoleInitParam& param)  {
        param.kvMap = kvMap;
        param.partitionId.CopyFrom(partitionId);
        return true;
    }
    bool doStartBuildFlow(const config::ResourceReaderPtr &resourceReader,
                        const KeyValueMap &kvMap,
                        const proto::PartitionId &partitionId,
                        BrokerFactory *brokerFactory,
                        BuildFlow::BuildFlowMode buildFlowMode,
                        WorkflowMode workflowMode,
                        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
    {
        return BuildFlow::startBuildFlow(resourceReader, kvMap, partitionId,
                    brokerFactory, buildFlowMode, workflowMode, metricProvider);
    }

    bool doSuspendReadAtTimesamp(
        int64_t timestamp, reader::RawDocumentReader::ExceedTsAction action)
    {
        return BuildFlow::suspendReadAtTimestamp(timestamp, action);
    }

    bool testIsStarted() const {
        return BuildFlow::isStarted();
    }
    
    bool testIsFinished() const {
        return BuildFlow::isFinished();
    }    
        
    builder::Builder *doGetBuilder() const {
        return BuildFlow::getBuilder();
    }

    processor::Processor *doGetProcessor() const {
        return BuildFlow::getProcessor();
    }

    reader::RawDocumentReader *doGetReader() const {
        return BuildFlow::getReader();
    }

    static BrokerFactory *createMockFactory() {
        return new NiceMockFlowFactory();
    }
    static MockBuildFlow *createNice() {
        return new NiceMockBuildFlow();
    }
private:
    BS_LOG_DECLARE();
};


}
}

#endif //ISEARCH_BS_MOCKBUILDFLOW_H
