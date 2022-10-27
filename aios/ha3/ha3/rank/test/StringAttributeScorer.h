#ifndef ISEARCH_STRINGATTRIBUTESCORER_H
#define ISEARCH_STRINGATTRIBUTESCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/Reference.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/rank/Scorer.h>
#include <autil/MultiValueType.h>
//#include <indexlib/index/attribute/string_attribute_reader.h>

IE_NAMESPACE_USE(index);
BEGIN_HA3_NAMESPACE(rank);

class StringAttributeScorer : public Scorer
{
public:
    StringAttributeScorer(const std::string &name = "StringAttributeScorer");
    ~StringAttributeScorer();

public:
    bool beginRequest(suez::turing::ScoringProvider *provider);
    score_t score(matchdoc::MatchDoc &matchDoc);
    void endRequest();
    void destroy();
    Scorer* clone();
private:
    std::string _strAttrName;
    const matchdoc::Reference<MatchData> *_matchDataRef;
    const matchdoc::Reference<autil::MultiChar> *_attrRef;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(StringAttributeScorer);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_STRINGATTRIBUTESCORER_H
