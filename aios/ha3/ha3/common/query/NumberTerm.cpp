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
#include "ha3/common/NumberTerm.h"

#include <sstream>
#include <stddef.h>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/analyzer/Token.h"
#include "ha3/common/Term.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, NumberTerm);

NumberTerm::NumberTerm(int64_t num,
                       const char *indexName,
                       const RequiredFields &requiredFields,
                       int64_t boost,
                       const string &truncateName)
    : Term("", indexName, requiredFields, boost, truncateName) {
    _leftNum = _rightNum = num;
    std::string tokenStr = StringUtil::toString(_leftNum);
    setWord(tokenStr.c_str());
}

NumberTerm::NumberTerm(int64_t leftNum,
                       bool leftInclusive,
                       int64_t rightNum,
                       bool rightInclusive,
                       const char *indexName,
                       const RequiredFields &requiredFields,
                       int64_t boost,
                       const string &truncateName)
    : common::Term("", indexName, requiredFields, boost, truncateName) {
    _leftNum = leftNum;
    _rightNum = rightNum;
    if (!leftInclusive) {
        _leftNum++;
    }
    if (!rightInclusive) {
        _rightNum--;
    }
    std::string tokenStr;
    if (_leftNum == _rightNum) {
        tokenStr = StringUtil::toString(_leftNum);
    } else {
        tokenStr
            = "[" + StringUtil::toString(_leftNum) + "," + StringUtil::toString(_rightNum) + "]";
    }
    setWord(tokenStr.c_str());
}

NumberTerm::~NumberTerm() {}

NumberTerm::NumberTerm(const NumberTerm &other)
    : Term(other) {
    _leftNum = other._leftNum;
    _rightNum = other._rightNum;
    std::string tokenStr;
    if (_leftNum == _rightNum) {
        tokenStr = StringUtil::toString(_leftNum);
    } else {
        tokenStr
            = "[" + StringUtil::toString(_leftNum) + "," + StringUtil::toString(_rightNum) + "]";
    }
    setWord(tokenStr.c_str());
}

bool NumberTerm::operator==(const Term &term) const {
    if (&term == this) {
        return true;
    }
    if (term.getTermName() != getTermName()) {
        return false;
    }
    const NumberTerm &term2 = dynamic_cast<const NumberTerm &>(term);
    if ((_requiredFields.fields.size() != term2._requiredFields.fields.size())
        || (_requiredFields.isRequiredAnd != term2._requiredFields.isRequiredAnd)) {
        return false;
    }
    for (size_t i = 0; i < _requiredFields.fields.size(); ++i) {
        if (_requiredFields.fields[i] != term2._requiredFields.fields[i]) {
            return false;
        }
    }

    return _leftNum == term2._leftNum && _rightNum == term2._rightNum
           && _indexName == term2._indexName && _token.getText() == term2._token.getText()
           && _boost == term2._boost && _truncateName == term2._truncateName;
}

std::string NumberTerm::toString() const {
    std::stringstream ss;
    ss << "NumberTerm: [" << _indexName << ",";
    ss << "[" << getLeftNumber() << ", " << getRightNumber() << "|";
    formatString(ss);
    ss << "]";
    return ss.str();
}

void NumberTerm::serialize(autil::DataBuffer &dataBuffer) const {
    Term::serialize(dataBuffer);
    dataBuffer.write(_leftNum);
    dataBuffer.write(_rightNum);
}

void NumberTerm::deserialize(autil::DataBuffer &dataBuffer) {
    Term::deserialize(dataBuffer);
    dataBuffer.read(_leftNum);
    dataBuffer.read(_rightNum);
}

} // namespace common
} // namespace isearch
