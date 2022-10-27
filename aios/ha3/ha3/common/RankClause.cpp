#include <ha3/common/RankClause.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, RankClause);

///////////////////////////////////////////////////////

RankClause::RankClause() { 
    _rankHint = NULL;
}

RankClause::~RankClause() { 
    DELETE_AND_SET_NULL(_rankHint);
}

void RankClause::setRankProfileName(const string &rankProfileName) {
    _rankProfileName = rankProfileName;
}

string RankClause::getRankProfileName() const {
    return _rankProfileName;
}

void RankClause::setFieldBoostDescription(const FieldBoostDescription& des) {
    _fieldBoostDescription = des;
}

const FieldBoostDescription& RankClause::getFieldBoostDescription() const {
    return _fieldBoostDescription;
}

void RankClause::setRankHint(RankHint *rankHint) {
    delete _rankHint;
    _rankHint = rankHint;
}

const RankHint* RankClause::getRankHint() const {
    return _rankHint;
}

void RankClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_rankProfileName);
    dataBuffer.write(_originalString);
    dataBuffer.write(_fieldBoostDescription);
    dataBuffer.write(_rankHint);
}

void RankClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_rankProfileName);
    dataBuffer.read(_originalString);
    dataBuffer.read(_fieldBoostDescription);
    dataBuffer.read(_rankHint);
}

string RankClause::toString() const {
    string rankClauseStr;
    rankClauseStr.append("rankprofilename:");
    rankClauseStr.append(_rankProfileName);
    rankClauseStr.append(",fieldboostdescription:");
    FieldBoostDescription::const_iterator packIter = 
        _fieldBoostDescription.begin();
    string fieldBoostDescStr;
    for (; packIter != _fieldBoostDescription.end(); packIter++) {
        string packageName = packIter->first;
        string boostInfo;
        SingleFieldBoostConfig::const_iterator fieldIter = packIter->second.begin();
        for (; fieldIter != packIter->second.end(); fieldIter++) {
            string fieldName = fieldIter->first;
            string fieldBoost = StringUtil::toString(fieldIter->second);
            boostInfo.append(fieldName);
            boostInfo.append("|");
            boostInfo.append(fieldBoost);
            boostInfo.append(",");
        }
        fieldBoostDescStr.append("package:[");
        fieldBoostDescStr.append(packageName);
        fieldBoostDescStr.append(":");
        fieldBoostDescStr.append(boostInfo);
        fieldBoostDescStr.append("]");
    }
    rankClauseStr.append(fieldBoostDescStr);
    if (NULL != _rankHint) {
        rankClauseStr.append(",rankhint:");
        rankClauseStr.append(_rankHint->toString());
    }
    return rankClauseStr;
}

END_HA3_NAMESPACE(common);

