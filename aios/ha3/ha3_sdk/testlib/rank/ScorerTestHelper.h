#ifndef ISEARCH_SCORERTESTHELPER_H
#define ISEARCH_SCORERTESTHELPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/rank/Scorer.h>
#include <suez/turing/expression/plugin/ScorerWrapper.h>
#include <ha3_sdk/testlib/common/FakeIndexParam.h>
#include <ha3_sdk/testlib/common/TestHelperBase.h>

BEGIN_HA3_NAMESPACE(rank);

class ScorerTestHelper : public HA3_NS(common)::TestHelperBase
{
public:
    ScorerTestHelper(const std::string &testPath);
    virtual ~ScorerTestHelper();
private:
    ScorerTestHelper(const ScorerTestHelper &);
    ScorerTestHelper& operator=(const ScorerTestHelper &);
public:
    virtual bool init(common::TestParamBase *param);
    bool beginRequest(Scorer *scorer);
    bool beginScore(Scorer *scorer);
    bool score(Scorer *scorer);
    bool batchScore(Scorer *scorer);
    bool endScore(Scorer *scorer);
    bool endRequest(Scorer *scorer);
public:
    ScoringProvider *getScoringProvider() {
        return _provider;
    }
    matchdoc::Reference<score_t>* getScoreRef() const {
        return _scoreRef;
    }

protected:
    suez::turing::ScorerWrapperPtr _scorerWrapper;
private:
    matchdoc::Reference<score_t> *_scoreRef;
    ScoringProvider *_provider;
    common::ScorerTestParam *_param;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ScorerTestHelper);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_SCORERTESTHELPER_H
