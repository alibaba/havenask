#include <ha3/qrs/IndexLimitQueryVisitor.h>
#include <ha3/common/Query.h>
#include <autil/StringUtil.h>
using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, IndexLimitQueryVisitor);

IndexLimitQueryVisitor::IndexLimitQueryVisitor(const std::string& indexName, 
        const std::string& termName){
    _indexName = indexName;
    _termName = termName;
    _isErrorPart = false;
}

IndexLimitQueryVisitor::~IndexLimitQueryVisitor() {
}

void IndexLimitQueryVisitor::addTerm(const Term& term, std::string& str){
    str.append(term.getIndexName());
    str.append(":");
    str.append(term.getWord().c_str());
    string termStr (term.getWord().c_str());
    if(termStr == _termName && term.getIndexName() == _indexName){
        _isErrorPart = true;
    }
}

void IndexLimitQueryVisitor::visitTermQuery(const TermQuery *query){
    const Term &term = query->getTerm();
    _queryStr.clear();
    addTerm(term, _queryStr);
    if(_isErrorPart){
        _subQueryStr = _queryStr;
    }
}

void IndexLimitQueryVisitor::visitPhraseQuery(const PhraseQuery *query){
    const PhraseQuery::TermArray &termArray = query->getTermArray();
    visitTermArray(termArray);
}

void IndexLimitQueryVisitor::visitMultiTermQuery(const MultiTermQuery *query) {
    const MultiTermQuery::TermArray &termArray = query->getTermArray();
    visitTermArray(termArray);
}

void IndexLimitQueryVisitor::visitAndQuery(const AndQuery *query){
    visitAdvancedQuery(query, "AND");
}

void IndexLimitQueryVisitor::visitOrQuery(const OrQuery *query){
    visitAdvancedQuery(query, "OR");
}

void IndexLimitQueryVisitor::visitAndNotQuery(const AndNotQuery *query){
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    assert(children->size() > 0);
    bool hasErrorPart = false;
    string str = "(";
    (*children)[0]->accept(this);
    if(_isErrorPart){
        hasErrorPart = true;
        _isErrorPart = false;
    }
    str += "(" + _queryStr + ")" + " ANDNOT (";
    _queryStr.clear();
    for (size_t i = 1; i < children->size(); ++i) {
        (*children)[i]->accept(this);
        if(i != children->size() - 1){
            str += _queryStr + " ";  
        }else{
            str += _queryStr;
        }
        _queryStr.clear();
        if(_isErrorPart){
            hasErrorPart = true;
            _isErrorPart = false;
        }
    }
    str += "))";
    _queryStr = str;
    if(hasErrorPart){
        _subQueryStr = _queryStr ;
    }
}

void IndexLimitQueryVisitor::visitRankQuery(const RankQuery *query){
    visitAdvancedQuery(query,"OR");
}

void IndexLimitQueryVisitor::visitNumberQuery(const NumberQuery *query){
    const NumberTerm &term = query->getTerm();
    _queryStr.clear();
    addTerm(term, _queryStr);
}

void IndexLimitQueryVisitor::visitAdvancedQuery(const Query *query, const string& prefix) {
    const vector<QueryPtr>* children = query->getChildQuery();
    vector<string> strVec;
    assert(children);
    bool hasErrorPart = false;
    for (size_t i = 0; i < children->size(); ++i) {
        (*children)[i]->accept(this);
        if(_queryStr == ""){
            continue;
        }        
        strVec.push_back(_queryStr);
        _queryStr.clear();
        if(_isErrorPart){
            hasErrorPart = true;
            _isErrorPart = false;
        }
    }
    string str;
    for (size_t i = 0; i < strVec.size(); ++i) {
        if(i != strVec.size() - 1){
            str += strVec[i] + " " + prefix + " ";  
        }else{
            str += strVec[i];
        }
    }
    if(str != ""){
        if(strVec.size() > 1){
            _queryStr = "(" + str +")"; 
        }else{
            _queryStr = str;
        }
    }
    if(hasErrorPart){
        _subQueryStr = _queryStr ;
    }

}

void IndexLimitQueryVisitor::visitTermArray(const vector<TermPtr> &termArray) {
    string str;
    bool hasErrorPart = false;
    for (size_t i = 0; i < termArray.size(); ++i) {
        addTerm(*termArray[i], str);
        str.append(" ");
        if(_isErrorPart){
            hasErrorPart = true;
            _isErrorPart = false;
        }
    }
    autil::StringUtil::trim(str);
    if(str != ""){
        _queryStr = "\"" + str +"\"";
    }
    else{
        _queryStr = "";
    }
    if(hasErrorPart){
        _subQueryStr = _queryStr;
    }
}

END_HA3_NAMESPACE(qrs);

