#ifndef ISEARCH_BS_TESTRAWDOCPRODUCER_H
#define ISEARCH_BS_TESTRAWDOCPRODUCER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/BrokerFactory.h"

namespace build_service {
namespace workflow {

class TestRawDocProducer : public RawDocProducer
{
public:
    TestRawDocProducer(const std::vector<document::RawDocumentPtr> &docVec);
    ~TestRawDocProducer();
private:
    TestRawDocProducer(const TestRawDocProducer &);
    TestRawDocProducer& operator=(const TestRawDocProducer &);
public:
    /* override */ FlowError produce(document::RawDocumentPtr &rawDoc);
    /* override */ bool seek(const common::Locator &locator);
    /* override */ bool stop();
private:
    std::vector<document::RawDocumentPtr> _docVec;
    size_t _cursor;
private:
    BS_LOG_DECLARE();
};


class TestBrokerFactory : public BrokerFactory
{
public:
    TestBrokerFactory(const std::vector<document::RawDocumentPtr> &docVec)
        : _docVec(docVec)
    {
    }
    ~TestBrokerFactory() {}
private:
    TestBrokerFactory(const TestBrokerFactory &);
    TestBrokerFactory& operator=(const TestBrokerFactory &);
public:
    /* override */ void stopProduceProcessedDoc() {}
public:
    /* override */ RawDocProducer *createRawDocProducer(const RoleInitParam &initParam) {
        return new TestRawDocProducer(_docVec);
    }
    /* override */ RawDocConsumer *createRawDocConsumer(const RoleInitParam &initParam) {
        return NULL;
    }

    /* override */ ProcessedDocProducer *createProcessedDocProducer(const RoleInitParam &initParam) {
        return NULL;
    }
    /* override */ ProcessedDocConsumer *createProcessedDocConsumer(const RoleInitParam &initParam) {
        return NULL;
    }

    bool initCounterMap(RoleInitParam &initParam) override
    { return true; }

private:
    std::vector<document::RawDocumentPtr> _docVec;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_TESTRAWDOCPRODUCER_H
