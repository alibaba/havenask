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
#include "ha3/common/Term.h"

#include <stddef.h>

#include "autil/DataBuffer.h"
#include "build_service/analyzer/Token.h"
#include "indexlib/document/extend_document/tokenize/analyzer_token.h"

#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, Term);

bool RequiredFields::operator==(const RequiredFields &other) const {
    if ((fields.size() != other.fields.size())
        ||(isRequiredAnd != other.isRequiredAnd))
    {
        return false;
    }
    for (size_t i = 0; i < fields.size(); ++i)
    {
        if (fields[i] != other.fields[i]) {
            return false;
        }
    }
    return true;
}

bool Term::operator == (const Term& term) const {
    return  _token == term._token
        && _indexName == term._indexName
        && _boost == term._boost
        && _truncateName == term._truncateName
        && _requiredFields == term._requiredFields;
}

std::string Term::toString() const {
    std::stringstream ss;
    ss << "Term:[" << _indexName << "|";
    formatString(ss);
    ss << "]";
    return ss.str();
}

void Term::formatString(std::stringstream &ss) const {
    bool isFirstField = true;
    string delimiter = _requiredFields.isRequiredAnd ? " and " : " or ";
    for (vector<string>::const_iterator it = _requiredFields.fields.begin();
         it != _requiredFields.fields.end(); ++it)
    {
        if (isFirstField) {
            ss << *it;
            isFirstField = false;
        } else {
            ss << delimiter << *it;
        }
    }
    ss << "|";

    ss << _token.getNormalizedText().c_str() << "|" << _boost << "|" <<
        _truncateName;
}

std::ostream& operator <<(std::ostream &os, const Term& term) {
    return os << term.toString();
}

void Term::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_token);
    dataBuffer.write(_indexName);
    dataBuffer.write(_requiredFields.fields);
    dataBuffer.write(_requiredFields.isRequiredAnd);
    dataBuffer.write(_boost);
    dataBuffer.write(_truncateName);
}

void Term::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_token);
    dataBuffer.read(_indexName);
    dataBuffer.read(_requiredFields.fields);
    dataBuffer.read(_requiredFields.isRequiredAnd);
    dataBuffer.read(_boost);
    dataBuffer.read(_truncateName);
}

} // namespace common
} // namespace isearch
