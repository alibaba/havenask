#include "build_service/workflow/TestBrokerFactory.h"

using namespace std;
namespace build_service {
namespace workflow {
BS_LOG_SETUP(workflow, TestRawDocProducer);

TestRawDocProducer::TestRawDocProducer(const vector<document::RawDocumentPtr> &docVec)
    : _docVec(docVec)
    , _cursor(0)
{
}

TestRawDocProducer::~TestRawDocProducer() {
}

FlowError TestRawDocProducer::produce(document::RawDocumentPtr &rawDoc) {
    if (_cursor >= _docVec.size()) {
        return FE_EOF;
    }
    rawDoc = _docVec[_cursor++];
    return FE_OK;
}

bool TestRawDocProducer::seek(const common::Locator &locator) {
    for (size_t i = 0; i < _docVec.size(); ++i) {
        if (_docVec[i]->getLocator() == locator) {
            _cursor = i;
            return true;
        }
    }
    return false;
}

bool TestRawDocProducer::stop() {
    return true;
}

}
}
