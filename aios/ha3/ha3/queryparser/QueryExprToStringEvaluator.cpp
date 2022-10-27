#include <ha3/queryparser/QueryExprToStringEvaluator.h>
#include <ha3/config/QueryInfo.h>
#include <assert.h>

using namespace std;
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, QueryExprToStringEvaluator);

QueryExprToStringEvaluator::QueryExprToStringEvaluator() {
}

QueryExprToStringEvaluator::~QueryExprToStringEvaluator() {
}

void QueryExprToStringEvaluator::evaluateAndExpr(AndQueryExpr *andExpr) {
    _ss << "{ANDExpr";
    string label = andExpr->getLabel();
    if (!label.empty()) {
        _ss << "@" << label;
    }
    _ss << ": ";
    andExpr->getLeftExpr()->evaluate(this);
    _ss << ", ";
    andExpr->getRightExpr()->evaluate(this);
    _ss << "}";
}

void QueryExprToStringEvaluator::evaluateOrExpr(OrQueryExpr *orExpr) {
    _ss << "{ORExpr";
    string label = orExpr->getLabel();
    if (!label.empty()) {
        _ss << "@" << label;
    }
    _ss << ": ";
    orExpr->getLeftExpr()->evaluate(this);
    _ss << ", ";
    orExpr->getRightExpr()->evaluate(this);
    _ss << "}";
}

void QueryExprToStringEvaluator::evaluateAndNotExpr(AndNotQueryExpr *andNotExpr) {
    _ss << "{ANDNotExpr";
    string label = andNotExpr->getLabel();
    if (!label.empty()) {
        _ss << "@" << label;
    }
    _ss << ": ";
    andNotExpr->getLeftExpr()->evaluate(this);
    _ss << ", ";
    andNotExpr->getRightExpr()->evaluate(this);
    _ss << "}";
}

void QueryExprToStringEvaluator::evaluateRankExpr(RankQueryExpr *rankExpr) {
    _ss << "{RankExpr";
    string label = rankExpr->getLabel();
    if (!label.empty()) {
        _ss << "@" << label;
    }
    _ss << ": ";
    rankExpr->getLeftExpr()->evaluate(this);
    _ss << ", ";
    rankExpr->getRightExpr()->evaluate(this);
    _ss << "}";
}


void QueryExprToStringEvaluator::evaluateWordsExpr(WordsTermExpr *wordsExpr) {
    _ss << "{WordsExpr";
    string label = wordsExpr->getLabel();
    if (!label.empty()) {
        _ss << "@" << label;
    }
    _ss << ": ";
    _ss << "(" << wordsExpr->getIndexName() << ")";
    _ss <<"'" << wordsExpr->getText() << "'}";
}

void QueryExprToStringEvaluator::evaluateNumberExpr(NumberTermExpr *numberExpr) {
    _ss << "{NumberExpr";
    string label = numberExpr->getLabel();
    if (!label.empty()) {
        _ss << "@" << label;
    }
    _ss << ": ";
    _ss << "(" << numberExpr->getIndexName() << ")";
    _ss << string(numberExpr->getLeftInclusive() ? "[" : "(")
        << numberExpr->getLeftNumber()
        << ", "
        << numberExpr->getRightNumber()
        << string(numberExpr->getRightInclusive() ? "]" : ")")
        << "}";

}

void QueryExprToStringEvaluator::evaluatePhraseExpr(PhraseTermExpr *phraseExpr) {
    _ss << "{PhraseExpr";
    string label = phraseExpr->getLabel();
    if (!label.empty()) {
        _ss << "@" << label;
    }
    _ss << ": ";
    _ss << "(" << phraseExpr->getIndexName() << ")";
    _ss <<"\"" << phraseExpr->getText() << "\"}";
}

void QueryExprToStringEvaluator::evaluateMultiTermExpr(
        MultiTermQueryExpr *multiTermExpr)
{
    if (multiTermExpr->getOpExpr() == OP_OR) {
        _ss << "{ORExpr";
    } else if (multiTermExpr->getOpExpr() == OP_AND) {
        _ss << "{ANDExpr";
    } else {
        _ss << multiTermExpr->getMinShouldMatch() << "{WEAKANDExpr";
    }
    string label = multiTermExpr->getLabel();
    if (!label.empty()) {
        _ss << "@" << label;
    }
    _ss << ": ";
    const MultiTermQueryExpr::TermExprArray& termQueryExprs = multiTermExpr->getTermExprs();
    size_t s = termQueryExprs.size();
    for (size_t i = 0; i < s; ++i) {
        termQueryExprs[i]->evaluate(this);
        if (i < s - 1) {
            _ss << ", ";
        }
    }
    _ss << "}";
}

void QueryExprToStringEvaluator::reset() {
    _ss.str("");
}

string QueryExprToStringEvaluator::getString() {
    return _ss.str();
}

string QueryExprToStringEvaluator::stealString() {
    string tmp =  _ss.str();
    reset();
    return tmp;
}

END_HA3_NAMESPACE(queryparser);
