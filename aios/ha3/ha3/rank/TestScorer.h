#ifndef ISEARCH_TESTSCORER_H
#define ISEARCH_TESTSCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/DefaultScorer.h>

BEGIN_HA3_NAMESPACE(rank);

class TestScorer : public DefaultScorer
{
public:
    TestScorer();
    ~TestScorer();
private:
    TestScorer& operator=(const TestScorer &);
public:
    Scorer* clone() {return new TestScorer(*this);}
    bool beginRequest(suez::turing::ScoringProvider *provider);
    score_t score(matchdoc::MatchDoc &matchDoc);
private:
    std::string _ip;
    matchdoc::Reference<int32_t> *_int32Ref;
    int32_t _count;;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_TESTSCORER_H
