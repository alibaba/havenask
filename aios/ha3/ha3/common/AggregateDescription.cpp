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
#include "ha3/common/AggregateDescription.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <set>

#include "alog/Logger.h"
#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/AggFunDescription.h"
#include "ha3/common/FilterClause.h"
#include "ha3/isearch.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

using namespace std;
using namespace autil;
using namespace suez::turing;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AggregateDescription);

AggregateDescription::AggregateDescription() : AggregateDescription("") {
}

AggregateDescription::AggregateDescription(const std::string &originalString)
    : _originalString(originalString), _groupKeyExpr(nullptr),
      _maxGroupCount(MAX_AGGREGATE_GROUP_COUNT), _filterClause(nullptr),
      _aggThreshold(0), _sampleStep(0), _sortType(AggSortType::AggSortByKey)
{
}

AggregateDescription::~AggregateDescription() {
    delete _groupKeyExpr;
    _groupKeyExpr = NULL;

    delete _filterClause;
    _filterClause = NULL;
    clearAggDescriptions();
}

void AggregateDescription::clearAggDescriptions() {
    for (size_t i = 0; i < _aggFunDescriptions.size(); ++i) {
        delete _aggFunDescriptions[i];
    }
    _aggFunDescriptions.clear();
}

std::string AggregateDescription::getOriginalString() const {
    return _originalString;
}

void AggregateDescription::setOriginalString(const std::string &originalString) {
    _originalString = originalString;
}

SyntaxExpr* AggregateDescription::getGroupKeyExpr() const {
    return _groupKeyExpr;
}

void AggregateDescription::setGroupKeyExpr(SyntaxExpr* groupKeyExpr) {
    if (_groupKeyExpr) {
        delete _groupKeyExpr;
        _groupKeyExpr = NULL;
    }
    _groupKeyExpr = groupKeyExpr;
}

bool AggregateDescription::isRangeAggregate() const {
    return !_ranges.empty();
}

FilterClause* AggregateDescription::getFilterClause() const {
    return _filterClause;
}

void AggregateDescription::setFilterClause(FilterClause* filterClause) {
    if(_filterClause) {
        delete _filterClause;
    }
    _filterClause = filterClause;
}

const vector<AggFunDescription*>& AggregateDescription::getAggFunDescriptions() const {
    return _aggFunDescriptions;
}

void AggregateDescription::setAggFunDescriptions(const vector<AggFunDescription*>& aggFunDescriptions) {
    clearAggDescriptions();
    set<string> funStrSet;
    for (vector<AggFunDescription*>::const_iterator it = aggFunDescriptions.begin();
         it != aggFunDescriptions.end(); ++it)
    {
        string funStr = (*it)->toString();
        if (funStrSet.find(funStr) != funStrSet.end()) {
            AUTIL_LOG(WARN, "The function has already existed, function[%s]",
                    funStr.c_str());
            delete *it;
            continue;
        }
        _aggFunDescriptions.push_back(*it);
        funStrSet.insert(funStr);
    }
}

void AggregateDescription::appendAggFunDescription(AggFunDescription* aggFunDescription) {
    _aggFunDescriptions.push_back(aggFunDescription);
}

const vector<string>& AggregateDescription::getRange() const {
    return _ranges;
}

void AggregateDescription::setRange(const vector<string>& ranges) {
    _ranges = ranges;
}

uint32_t AggregateDescription::getMaxGroupCount() const {
    return _maxGroupCount > 0 ? _maxGroupCount : 0;
}

void AggregateDescription::setMaxGroupCount(int32_t maxGroupCount) {
    _maxGroupCount = std::max(0, maxGroupCount);
}

void AggregateDescription::setMaxGroupCount(const std::string &maxGroupCountStr) {
    int32_t maxGroupCount = MAX_AGGREGATE_GROUP_COUNT;
    StringUtil::strToInt32(maxGroupCountStr.c_str(), maxGroupCount);
    setMaxGroupCount(maxGroupCount);
}

uint32_t AggregateDescription::getAggThreshold() const {
    return _aggThreshold;
}

void AggregateDescription::setAggThreshold(uint32_t aggThreshold) {
    _aggThreshold = aggThreshold;
}

void AggregateDescription::setAggThreshold(const std::string &aggThresholdStr) {
    int32_t aggThreshold = 0;
    autil::StringUtil::strToInt32(aggThresholdStr.c_str(), aggThreshold);
    if (aggThreshold >= 0) {
        _aggThreshold = aggThreshold;
    }
}

uint32_t AggregateDescription::getSampleStep() const {
    return _sampleStep;
}

void AggregateDescription::setSampleStep(uint32_t sampleStep) {
    _sampleStep = sampleStep;
}

void AggregateDescription::setSampleStep(const string &sampleStepStr) {
    int32_t sampleStep = 1;
    StringUtil::strToInt32(sampleStepStr.c_str(), sampleStep);
    if (sampleStep >= 1) {
        _sampleStep = sampleStep;
    }
}

AggSortType AggregateDescription::getSortType() const {
    return _sortType;
}

void AggregateDescription::setSortType(AggSortType sortType) {
    _sortType = sortType;
}

void AggregateDescription::setSortType(const std::string &sortTypeStr) {
    if ("count" == sortTypeStr) {
        _sortType = AggSortType::AggSortByCount;
    }
}


void AggregateDescription::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_originalString);
    dataBuffer.write(_groupKeyExpr);
    dataBuffer.write(_aggFunDescriptions);
    dataBuffer.write(_ranges);
    dataBuffer.write(_maxGroupCount);
    dataBuffer.write(_filterClause);
    dataBuffer.write(_aggThreshold);
    dataBuffer.write(_sampleStep);
    // _sortType used only in qrs, no need to serialize
}

void AggregateDescription::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_originalString);
    dataBuffer.read(_groupKeyExpr);
    dataBuffer.read(_aggFunDescriptions);
    dataBuffer.read(_ranges);
    dataBuffer.read(_maxGroupCount);
    dataBuffer.read(_filterClause);
    dataBuffer.read(_aggThreshold);
    dataBuffer.read(_sampleStep);
}

std::string AggregateDescription::toString() const {
    assert(_groupKeyExpr);
    string aggDescription;
    aggDescription.append("group_key:");
    aggDescription.append(_groupKeyExpr->getExprString());
    aggDescription.append(",agg_fun:");
    size_t aggFunSize = _aggFunDescriptions.size();
    for (size_t i = 0; i < aggFunSize; i++) {
        assert(_aggFunDescriptions[i]);
        aggDescription.append(_aggFunDescriptions[i]->toString());
        aggDescription.append("#");
    }
    aggDescription.append(",ranges:");
    for (size_t i = 0; i < _ranges.size(); i++) {
        aggDescription.append(_ranges[i]);
        aggDescription.append("|");
    }
    aggDescription.append(",maxgroupcount:");
    aggDescription.append(StringUtil::toString(_maxGroupCount));
    aggDescription.append(",step:");
    aggDescription.append(StringUtil::toString(_sampleStep));
    aggDescription.append(",thredshold:");
    aggDescription.append(StringUtil::toString(_aggThreshold));
    if (_filterClause) {
        aggDescription.append(",filter:");
        aggDescription.append(_filterClause->toString());
    }
    if (_sortType == AggSortType::AggSortByCount) {
        aggDescription.append(",order_by:count");
    }
    return aggDescription;
}

} // namespace common
} // namespace isearch
