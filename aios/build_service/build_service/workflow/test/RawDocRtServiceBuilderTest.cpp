#include "build_service/test/unittest.h"
#include "build_service/workflow/RawDocRtServiceBuilderImpl.h"
#include "build_service/workflow/DocReaderProducer.h"
#include "build_service/workflow/test/MockIndexPartition.h"
#include "build_service/builder/test/MockBuilder.h"
#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/util/SwiftClientCreator.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::builder;
using namespace build_service::common;
using namespace build_service::util;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index);

namespace build_service {
namespace workflow {

class MockDocReaderProducer : public DocReaderProducer
{
public:
    MockDocReaderProducer()
        : DocReaderProducer(nullptr, 1) {}
    ~MockDocReaderProducer() {}
public:    
    MOCK_METHOD1(produce, FlowError(document::RawDocumentPtr&));
    MOCK_METHOD1(seek, bool(const common::Locator &locator));
    MOCK_METHOD0(stop, bool());
    MOCK_METHOD1(getMaxTimestamp, bool(int64_t&));
    MOCK_METHOD1(getLastReadTimestamp, bool(int64_t&)); 
};


class FakeRawDocRtServiceBuilderImpl : public RawDocRtServiceBuilderImpl
{

public:
    FakeRawDocRtServiceBuilderImpl(
        const std::string &configPath,
        const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart,
        const RealtimeBuilderResource& builderResource)
        : RawDocRtServiceBuilderImpl(configPath, indexPart, builderResource)
    {}
    ~FakeRawDocRtServiceBuilderImpl() {}
public:
    void externalActions() override {}
    
};
    
typedef ::testing::StrictMock<MockDocReaderProducer> StrictMockDocReaderProducer;
typedef ::testing::NiceMock<MockDocReaderProducer> NiceMockDocReaderProducer;


ACTION_P2(SET0_AND_RETURN, param0, ret) {
    arg0 = param0;
    return ret;
}

class RawDocRtServiceBuilderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
    
protected:
    common::Locator createLocator(int64_t timestamp) {
        Locator locator(timestamp);
        return locator;
            
    }
};

void RawDocRtServiceBuilderTest::setUp() {
}

void RawDocRtServiceBuilderTest::tearDown() {
}

TEST_F(RawDocRtServiceBuilderTest, testForceSeek) {
    string configPath = TEST_DATA_PATH"/admin_test/config_with_control_config";
    MockIndexPartition* _mockIndexPartition = new NiceMockIndexPartition();
    IndexPartitionPtr _indexPartition(_mockIndexPartition);

    unique_ptr<MockDocReaderProducer> _mockProducer(new NiceMockDocReaderProducer());
    unique_ptr<MockBuilder> _mockBuilder(new NiceMockBuilder);

    BuildFlowThreadResource threadResource;
    RealtimeBuilderResource buildResource(kmonitor::MetricsReporterPtr(),
                                          IE_NAMESPACE(util)::TaskSchedulerPtr(new IE_NAMESPACE(util)::TaskScheduler),
                                          SwiftClientCreatorPtr(new SwiftClientCreator),
                                          threadResource);
    
    unique_ptr<FakeRawDocRtServiceBuilderImpl> _realtimeBuilder(
        new FakeRawDocRtServiceBuilderImpl(configPath, _indexPartition, buildResource));
    
    _realtimeBuilder->_builder = _mockBuilder.get();
    _realtimeBuilder->_producer = _mockProducer.get();
    _realtimeBuilder->_supportForceSeek = true;
    _realtimeBuilder->_seekToLatestWhenRecoverFail = true;

    EXPECT_CALL(*_mockProducer, getLastReadTimestamp(_)).WillOnce(
            SET0_AND_RETURN(10, true));

    EXPECT_CALL(*_mockBuilder, getIncVersionTimestampNonBlocking(_)).WillRepeatedly(
            SET0_AND_RETURN(21, Builder::RS_OK));
    EXPECT_CALL(*_mockBuilder, getLastLocatorNonBlocking(_)).WillRepeatedly(
            SET0_AND_RETURN(createLocator(20), Builder::RS_OK));
    _realtimeBuilder->_maxRecoverTime = 0;
    _realtimeBuilder->_startRecoverTime = TimeUtility::currentTimeInSeconds() - 1; // early 1s
    _realtimeBuilder->_isRecovered = false;
    EXPECT_EQ(-1, _realtimeBuilder->_indexPartition->mForceSeekInfo.first);
    EXPECT_EQ(-1, _realtimeBuilder->_indexPartition->mForceSeekInfo.second);
    EXPECT_CALL(*_mockProducer, getMaxTimestamp(_))
        .WillOnce(DoAll(SetArgReferee<0>(1000), Return(true)));
    common::Locator targetLocator;
    targetLocator.setSrc(_mockProducer->getLocatorSrc());
    targetLocator.setOffset(1000);
    EXPECT_CALL(*_mockProducer, seek(targetLocator)).WillOnce(Return(true));
    _realtimeBuilder->executeBuildControlTask();
    EXPECT_EQ(ERROR_NONE, _realtimeBuilder->_errorCode);
    EXPECT_TRUE(_realtimeBuilder->_isRecovered);
    EXPECT_EQ(_realtimeBuilder->_indexPartition->mForceSeekInfo.first, 10);
    EXPECT_EQ(_realtimeBuilder->_indexPartition->mForceSeekInfo.second, 1000);
    
}

}
}
