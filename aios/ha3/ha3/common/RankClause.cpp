/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/common/RankClause.h"

#include <cstddef>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/RankHint.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, RankClause);

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

} // namespace common
} // namespace isearch
