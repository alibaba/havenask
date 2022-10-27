#include <ha3_sdk/testlib/rank/ScorerTestHelper.h>
#include <autil/StringUtil.h>
#include <ha3/common/Ha3MatchDocAllocator.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, ScorerTestHelper);

ScorerTestHelper::ScorerTestHelper(const std::string &testPath) : TestHelperBase(testPath) {
    _scoreRef = NULL;
    _param = NULL;
    _provider = NULL;
}

ScorerTestHelper::~ScorerTestHelper() {
    POOL_DELETE_CLASS(_provider);
}

bool ScorerTestHelper::init(TestParamBase *param) {
    _param = (ScorerTestParam *)param;

    _scoreRef = _mdAllocatorPtr->declare<score_t>(SCORE_REF);

    if (!TestHelperBase::init(_param)) {
        return false;
    }

    RankResource resource;
    fillSearchPluginResource(&resource);
    resource.indexReaderPtr = _indexPartReader->GetIndexReader();

    _provider = POOL_NEW_CLASS(_pool, ScoringProvider, resource);
    _providerBase = _provider;
    return _provider != NULL;
}

bool ScorerTestHelper::beginRequest(Scorer *scorer) {
    _scorerWrapper.reset(new suez::turing::ScorerWrapper(scorer, _scoreRef));

    bool ret = _scorerWrapper->beginRequest(_provider);
    if (ret) {
        allocateMatchDocs(_param->docIdStr, _matchDocs);

        if (_matchDataRef != NULL && !_param->matchDataStr.empty()) {
            fillMatchDatas(_param->matchDataStr, _matchDocs);
        }
        if (_simpleMatchDataRef != NULL && !_param->simpleMatchDataStr.empty()) {
            fillSimpleMatchDatas(_param->simpleMatchDataStr, _matchDocs);
        }
    }
    return ret;
}

bool ScorerTestHelper::beginScore(Scorer *scorer) {
    return true;
}

bool ScorerTestHelper::score(Scorer *scorer) {
    vector<score_t> expectedScores;

    StringUtil::fromString(_param->expectedScoreStr, expectedScores, ",");
    if (_matchDocs.size() != expectedScores.size()) {
        return false;
    }
    bool allEqual = true;
    for (size_t i = 0; i < _matchDocs.size(); i++) {
        score_t score = _scorerWrapper->score(_matchDocs[i]);
        if (std::fabs(score - expectedScores[i]) > 1e-5) {
            allEqual = false;
        }
    }
    return allEqual;
}

bool ScorerTestHelper::batchScore(Scorer *scorer) {
    vector<score_t> expectedScores;

    StringUtil::fromString(_param->expectedScoreStr, expectedScores, ",");
    if (_matchDocs.size() != expectedScores.size()) {
        return false;
    }

    _scorerWrapper->batchScore(&_matchDocs[0], _matchDocs.size());

    bool allEqual = true;
    for (size_t i = 0; i < _matchDocs.size(); i++) {
        score_t score = 0.0;
        score = _scoreRef->get(_matchDocs[i]);
        if (std::fabs(score - expectedScores[i]) > 1e-5) {
            allEqual = false;
        }
    }
    return allEqual;
}

bool ScorerTestHelper::endScore(Scorer *scorer) {
    return true;
}

bool ScorerTestHelper::endRequest(Scorer *scorer) {
    _scorerWrapper->endRequest();
    return true;
}

END_HA3_NAMESPACE(rank);
