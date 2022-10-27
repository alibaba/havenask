#include <ha3/summary/SummaryExtractorChain.h>

using namespace std;
BEGIN_HA3_NAMESPACE(summary);
HA3_LOG_SETUP(summary, SummaryExtractorChain);

SummaryExtractorChain::SummaryExtractorChain() { 
}

SummaryExtractorChain::SummaryExtractorChain(const SummaryExtractorChain &other)
{
    for (ConstIterator it = other._extractors.begin(); 
         it != other._extractors.end(); it++)
    {
        _extractors.push_back((*it)->clone());
    }
}

SummaryExtractorChain::~SummaryExtractorChain() { 
    for (Iterator it = _extractors.begin(); it != _extractors.end(); it++) {
        (*it)->destory();
    }
    _extractors.clear();
}

void SummaryExtractorChain::addSummaryExtractor(SummaryExtractor *extractor) {
    _extractors.push_back(extractor);
}

SummaryExtractorChain* SummaryExtractorChain::clone() const {
    SummaryExtractorChain *chain = new SummaryExtractorChain(*this);
    return chain;
}

void SummaryExtractorChain::destroy() {
    delete this;
}

bool SummaryExtractorChain::beginRequest(SummaryExtractorProvider *provider) {
    for (Iterator it = _extractors.begin(); it != _extractors.end(); it++) {
        if (!(*it)->beginRequest(provider)) {
            return false;
        }
    }
    return true;
}

void SummaryExtractorChain::extractSummary(common::SummaryHit &summaryHit) {
    for (Iterator it = _extractors.begin(); it != _extractors.end(); it++) {
        (*it)->extractSummary(summaryHit);
    }
}
    
void SummaryExtractorChain::endRequest() {
    for (Iterator it = _extractors.begin(); it != _extractors.end(); it++) {
        (*it)->endRequest();
    }
}

END_HA3_NAMESPACE(summary);

