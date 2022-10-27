#ifndef ISEARCH_BS_SWIFTBROKERFACTORY_H
#define ISEARCH_BS_SWIFTBROKERFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/BrokerFactory.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "build_service/util/SwiftClientCreator.h"
#include "build_service/config/SwiftConfig.h"

namespace build_service {
namespace workflow {

class SwiftBrokerFactory : public BrokerFactory
{
public:
    SwiftBrokerFactory(const util::SwiftClientCreatorPtr& swiftClientCreator);
    ~SwiftBrokerFactory();
private:
    SwiftBrokerFactory(const SwiftBrokerFactory &);
    SwiftBrokerFactory& operator=(const SwiftBrokerFactory &);
public:
    /* override */ void stopProduceProcessedDoc();
public:
    /* override */ RawDocProducer *createRawDocProducer(const RoleInitParam &initParam);
    /* override */ RawDocConsumer *createRawDocConsumer(const RoleInitParam &initParam);

    /* override */ ProcessedDocProducer *createProcessedDocProducer(const RoleInitParam &initParam);
    /* override */ ProcessedDocConsumer *createProcessedDocConsumer(const RoleInitParam &initParam);
    /* override*/ bool initCounterMap(RoleInitParam &initParam);
    
private:
    std::map<std::string, SwiftWriterWithMetric> createSwiftWriters(
            const RoleInitParam &initParam);
    bool createSwiftReaderConfig(const RoleInitParam &initParam,
                                 const config::SwiftConfig& swiftConfig,
                                 std::string &swiftReadConfig);
    bool initSwiftProcessedDocProducer(const RoleInitParam &initParam,
                                       SwiftProcessedDocProducer* producer);

    std::string getClusterName(const RoleInitParam &initParam);
    
protected:
    virtual SWIFT_NS(client)::SwiftClient *createClient(const std::string &zfsRootPath,
            const std::string &swiftClientConfig);
    virtual SWIFT_NS(client)::SwiftWriter *createWriter(
            SWIFT_NS(client)::SwiftClient* client,
            const std::string &topicName,
            const std::string &writerConfig);
    virtual SwiftProcessedDocProducer *doCreateProcessedDocProducer(
        SWIFT_NS(client)::SwiftReader *reader, const RoleInitParam &initParam);
    virtual SWIFT_NS(client)::SwiftReader *createReader(SWIFT_NS(client)::SwiftClient* client,
            const std::string &readerConfig)
    {
        return client->createReader(readerConfig, NULL);
    }
private:
    // must keep in member
    util::SwiftClientCreatorPtr _swiftClientCreator;

    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftBrokerFactory);

}
}

#endif //ISEARCH_BS_SWIFTBROKERFACTORY_H
