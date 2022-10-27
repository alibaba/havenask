#ifndef ISEARCH_BS_MOCKSWIFTBROKERFACTORY_H
#define ISEARCH_BS_MOCKSWIFTBROKERFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/SwiftBrokerFactory.h"

namespace build_service {
namespace workflow {

class MockSwiftBrokerFactory : public SwiftBrokerFactory
{
public:
    MockSwiftBrokerFactory()
        : SwiftBrokerFactory(util::SwiftClientCreatorPtr(new util::SwiftClientCreator))
    {}
    ~MockSwiftBrokerFactory() {}
    /* MockConsumer() { */
    /* } */
    /* ~MockConsumer() { */
    /* } */
public:
    MOCK_METHOD0(stopProduceProcessedDoc, void());
    MOCK_METHOD1(createRawDocProducer, RawDocProducer*(const RoleInitParam &));
    MOCK_METHOD1(createRawDocConsumer, RawDocConsumer*(const RoleInitParam &));
    MOCK_METHOD1(createProcessedDocProducer, ProcessedDocProducer*(const RoleInitParam &));
    MOCK_METHOD1(createProcessedDocConsumer, ProcessedDocConsumer*(const RoleInitParam &));
};

typedef ::testing::StrictMock<MockSwiftBrokerFactory> StrictMockSwiftBrokerFactory;
typedef ::testing::NiceMock<MockSwiftBrokerFactory> NiceMockSwiftBrokerFactory;

}
}

#endif //ISEARCH_BS_MOCKCONSUMER_H
