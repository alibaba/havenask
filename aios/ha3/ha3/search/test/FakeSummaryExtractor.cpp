#include <ha3/search/test/FakeSummaryExtractor.h>

using namespace std;
USE_HA3_NAMESPACE(summary);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FakeSummaryExtractor);

FakeSummaryExtractor::FakeSummaryExtractor() {
    _isBeginRequestSuccess = true;
}

FakeSummaryExtractor::~FakeSummaryExtractor() {
}

SummaryExtractor* FakeSummaryExtractor::clone() {
    return new FakeSummaryExtractor(*this);
}

void FakeSummaryExtractor::destory() {
    delete this;
}

bool FakeSummaryExtractor::beginRequest(SummaryExtractorProvider *provider) {
    if (!_isBeginRequestSuccess) {
        return false;
    }

    _values.push_back(new char);
    
    _p = new char(10);

    for (vector<string>::const_iterator it = _attributes.begin(); 
         it != _attributes.end(); ++it)
    {
        if (!provider->fillAttributeToSummary(*it)) {
            return false;
        }
    }
    return true;
}

void FakeSummaryExtractor::endRequest() {
    DELETE_AND_SET_NULL(_p);
    for (size_t i = 0; i < _values.size(); i++) {
        delete _values[i];
        _values[i] = NULL;
    }
}

void FakeSummaryExtractor::extractSummary(common::SummaryHit &summaryHit) {
    if (_values.size() > 0) {
        *(_values[0]) = 'c';
    }
    return;
}

END_HA3_NAMESPACE(search);

