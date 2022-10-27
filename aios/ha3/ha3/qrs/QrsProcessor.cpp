#include <ha3/qrs/QrsProcessor.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <string>

using namespace std;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, QrsProcessor);

QrsProcessor::QrsProcessor() { 
    _tracer = NULL;
    _timeoutTerminator = NULL;
}

QrsProcessor::QrsProcessor(const QrsProcessor &processor) { 
    _keyValues = processor._keyValues;
    _nextProcessor.reset();
    _tracer = NULL;
    _timeoutTerminator = NULL;
}

QrsProcessor::~QrsProcessor() { 
}

void QrsProcessor::process(RequestPtr &requestPtr, 
                           ResultPtr &resultPtr) 
{
    assert(resultPtr);
    if (resultPtr->hasError()) {
        return;
    }
    if(_nextProcessor.get()) {
        _nextProcessor->process(requestPtr, resultPtr);
    }
}

bool QrsProcessor::init(const QrsProcessorInitParam &param) {
    return init(*param.keyValues, param.resourceReader);
}

bool QrsProcessor::init(const KeyValueMap &keyValues,
                        config::ResourceReader *resourceReader) 
{
    _keyValues = keyValues;
    return true;
}

QrsProcessorPtr QrsProcessor::getNextProcessor()
{
    return _nextProcessor;
}

void QrsProcessor::setNextProcessor(QrsProcessorPtr processor) {
    _nextProcessor = processor;
}

void QrsProcessor::setTimeoutTerminator(common::TimeoutTerminator *timeoutTerminator) {
    _timeoutTerminator = timeoutTerminator;
    if (_nextProcessor) {
        _nextProcessor->setTimeoutTerminator(timeoutTerminator);
    }
}

string QrsProcessor::getName() const {
    return "QrsProcessor";
}

const KeyValueMap& QrsProcessor::getParams() const {
    return _keyValues;
}

string QrsProcessor::getParam(const string &key) const {
    KeyValueMap::const_iterator it = _keyValues.find(key);
    if (it != _keyValues.end()) {
        return it->second;
    }
    return "";
}

void QrsProcessor::fillSummary(const RequestPtr &requestPtr,
                               const ResultPtr &resultPtr)
{
    if(_nextProcessor) {
        _nextProcessor->fillSummary(requestPtr, resultPtr);
    }
}

END_HA3_NAMESPACE(qrs);

