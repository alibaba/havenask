#include <ha3_sdk/example/scorer/ScorerSample.h>
#include <math.h>
#include <ha3/rank/MatchData.h>

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(rank);
//LOG_SETUP(rank, ScorerSample);

ScorerSample::ScorerSample(const ScorerSample &other) 
    : _matchDataRef(other._matchDataRef)
    , _priceRef(other._priceRef)
    , _salesCountRef(other._salesCountRef)
    , _isBatchScoreRef(other._isBatchScoreRef)
    , _scorerConf(other._scorerConf)
{
}

Scorer* ScorerSample::clone() {
    return new ScorerSample(*this);
}

bool ScorerSample::beginRequest(suez::turing::ScoringProvider *provider_)
{
    auto provider = dynamic_cast<ScoringProvider *>(provider_);
    _matchDataRef = provider->requireMatchData();
    _priceRef = provider->requireAttribute<int32_t>("price");//'price' is attribute name 
    _salesCountRef = provider->requireAttribute<int32_t>("sales_count");
    _isBatchScoreRef = provider->declareGlobalVariable<bool>("isBatchScore");
    // AuxTableReaderTyped<int> *test = provider->createAuxTableReader<int>("aux_sales_count");
    // assert(test); (void)test;
    // delete test;
    *_isBatchScoreRef = false;
    return (_priceRef != NULL 
            && _salesCountRef != NULL 
            && _matchDataRef != NULL
            && _isBatchScoreRef != NULL);
}

score_t ScorerSample::score(matchdoc::MatchDoc& matchDoc) {
    score_t  score = 0.0f;

    // get 'price' attribute
    int32_t price = 0;
    price = _priceRef->get(matchDoc);

    // get 'sales_count' attribute
    int32_t salesCount = 0;
    salesCount = _salesCountRef->get(matchDoc);
    
    // get 'MatchData' 
    const MatchData &matchData = 
        _matchDataRef->getReference(matchDoc);
    uint32_t matchedTermCount = 0;
    for (uint32_t i = 0; i < matchData.getNumTerms(); i++) {
        const TermMatchData &tmd = matchData.getTermMatchData(i);
        if (tmd.isMatched()) {
            matchedTermCount++;
        }
    }

    //compute score
    score = (score_t)(price * salesCount * matchedTermCount);
    
    return score;
}

void ScorerSample::batchScore(ScorerParam &scorerParam) {
    *_isBatchScoreRef = true;
    Scorer::batchScore(scorerParam);
}

void ScorerSample::endRequest() {
    //free the resource allocate in 'BeginRequest()' 
}

void ScorerSample::destroy() {
    delete this;
}

END_HA3_NAMESPACE(rank);
