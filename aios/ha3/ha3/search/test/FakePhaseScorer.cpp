#include <ha3/search/test/FakePhaseScorer.h>
#include <assert.h>
#include <ha3/common/Request.h>

using namespace std;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FakePhaseScorer);

FakePhaseScorer::FakePhaseScorer(const std::string &name) { 
    _name = name;
    _scoreRef = NULL;
    _typeRef = NULL;
    _stringRef = NULL;
    _auxTableReader = NULL;
}

FakePhaseScorer::~FakePhaseScorer() { 
    delete _auxTableReader;
}

bool FakePhaseScorer::beginRequest(suez::turing::ScoringProvider *provider_) 
{
    auto provider = dynamic_cast<ScoringProvider *>(provider_);
    bool ret = true;
    const Request* request = provider->getRequest();
    const ConfigClause *configClause = request->getConfigClause();
    if (getValue<bool>(configClause, "match_data")) {
        ret = ret && (provider->requireMatchData() != NULL);
    }
    if (getValue<bool>(configClause, "simple_match_data")) {
        ret = ret && (provider->requireSimpleMatchData() != NULL);
    }
    if (getValue<bool>(configClause, "declare_variable")) {
        provider->declareVariable<int32_t>("abc");
    }
    if (getValue<bool>(configClause, "declare_globalvariable")) {
        provider->declareGlobalVariable<int32_t>("abc");
    }
    _typeRef = provider->requireAttribute<int32_t>("type");
    string scoreAttrName = getValue<string>(configClause, "score");
    if (!scoreAttrName.empty()) {
        _scoreRef = provider->requireAttribute<score_t>(scoreAttrName);
    }
    if (!getValue<bool>(configClause, "no_declare_string")) {
        _stringRef = provider->declareVariable<string>("abcdef");
    }
    string auxTableRead = getValue<string>(configClause, "aux_key");
    if (!auxTableRead.empty()) {
        _auxTableReader = provider->createAuxTableReader<int32_t>("aux_attr");
        int32_t value = 0;
        if (_auxTableReader && _auxTableReader->GetValue(auxTableRead, value)) {
            *provider->declareGlobalVariable<int32_t>("aux_value", true) = value;
        }
    }
    _requestTracer = provider->getRequestTracer();

    REQUEST_TRACE_WITH_TRACER(_requestTracer, TRACE1, "begin request in scorer[%s]", _name.c_str());

    _terminateSeekCount = getValue<int32_t>(configClause, "terminate_seek");
    _queryTerminator = provider->getQueryTerminator();
    TRACE_SETUP(provider);
    return ret && (_scoreRef || _typeRef);
}

score_t FakePhaseScorer::score(matchdoc::MatchDoc &matchDoc) {
    if (--_terminateSeekCount == 0) {
        _queryTerminator->setTerminated(true);
    }
    score_t score = 0;
    if (_scoreRef) {
        score = _scoreRef->get(matchDoc);
    } else if (_typeRef) {
        score = _typeRef->get(matchDoc);
    }
    if (_stringRef) {
        _stringRef->set(matchDoc, string("abc"));
    }
    RANK_TRACE(TRACE1, matchDoc, "rank trace for [%s]", _name.c_str());    
    return score;
}

void FakePhaseScorer::endRequest() {
    REQUEST_TRACE_WITH_TRACER(_requestTracer, TRACE1, "end request in scorer[%s]", _name.c_str());
}

void FakePhaseScorer::destroy() {
    delete this;
}

Scorer* FakePhaseScorer::clone() {
    return new FakePhaseScorer(*this);
}


END_HA3_NAMESPACE(search);

