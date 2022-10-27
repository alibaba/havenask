#ifndef ISEARCH_BS_MOCKFLOWFACTORY_H
#define ISEARCH_BS_MOCKFLOWFACTORY_H

#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/test/test.h"
#include "build_service/workflow/FlowFactory.h"
#include "build_service/reader/test/MockRawDocumentReader.h"
#include "build_service/processor/test/MockProcessor.h"
#include "build_service/builder/test/MockBuilder.h"
#include "build_service/builder/test/MockIndexBuilder.h"

using ::testing::ReturnRef;
namespace build_service {
namespace workflow {

ACTION(CREATE_READER) {
    reader::RawDocumentReader *reader = new reader::NiceMockRawDocumentReader;
    return reader;
}

ACTION(CREATE_PROCESSOR) {
    return new processor::NiceMockProcessor;
}

ACTION(CREATE_BUILDER) {
    auto mockBuilder = new builder::NiceMockBuilder;
    IE_NAMESPACE(config)::IndexPartitionOptions options;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema;
    auto mockIndexBuilder = new IE_NAMESPACE(index)::MockIndexBuilder(options, "", schema);
    static IE_NAMESPACE(util)::CounterMapPtr MockBuilderCounterMap(
        new IE_NAMESPACE(util)::CounterMap);
    EXPECT_CALL(*mockIndexBuilder, GetCounterMap()).
        WillRepeatedly(ReturnRef(MockBuilderCounterMap));
    mockBuilder->_indexBuilder.reset(mockIndexBuilder);
    return mockBuilder;
}

class MockFlowFactory : public FlowFactory
{
public:
    MockFlowFactory()
        : FlowFactory(util::SwiftClientCreatorPtr())
    {
        ON_CALL(*this, createReader(_)).WillByDefault(CREATE_READER());
        ON_CALL(*this, createProcessor(_)).WillByDefault(CREATE_PROCESSOR());
        ON_CALL(*this, createBuilder(_)).WillByDefault(CREATE_BUILDER());

        ON_CALL(*this, createRawDocProducer(_)).WillByDefault(
                Invoke(this, &MockFlowFactory::doCreateRawDocProducer));
        
        ON_CALL(*this, createRawDocConsumer(_)).WillByDefault(
                Invoke(this, &MockFlowFactory::doCreateRawDocConsumer)); 
    }
    ~MockFlowFactory() {}
public:
    MOCK_METHOD1(createRawDocProducer, RawDocProducer*(const RoleInitParam&));
    MOCK_METHOD1(createRawDocConsumer, RawDocConsumer*(const RoleInitParam&)); 

private:
    MOCK_METHOD1(createReader, reader::RawDocumentReader*(const RoleInitParam&));
    MOCK_METHOD1(createProcessor, processor::Processor*(const RoleInitParam&));
    MOCK_METHOD1(createBuilder, builder::Builder*(const RoleInitParam&));
private:
    RawDocProducer *doCreateRawDocProducer(const RoleInitParam &initParam) {
        return FlowFactory::createRawDocProducer(initParam);
    }
    RawDocConsumer *doCreateRawDocConsumer(const RoleInitParam &initParam) {
        return FlowFactory::createRawDocConsumer(initParam);
    }
    
};

typedef ::testing::StrictMock<MockFlowFactory> StrictMockFlowFactory;
typedef ::testing::NiceMock<MockFlowFactory> NiceMockFlowFactory;

}
}

#endif //ISEARCH_BS_MOCKFLOWFACTORY_H
