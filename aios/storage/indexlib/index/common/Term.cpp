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
#include "indexlib/index/common/Term.h"

#include <sstream>

using namespace std;

namespace indexlib::index {

bool Term::operator==(const Term& term) const
{
    if (&term == this) {
        return true;
    }

    if (_word != term._word || _indexName != term._indexName || _isNull != term._isNull ||
        _hintValues != term._hintValues) {
        return false;
    }
    return _truncateName == term._truncateName && _liteTerm == term._liteTerm;
}

std::string Term::ToString() const
{
    std::stringstream ss;
    ss << "Term:[" << _indexName << "," << _truncateName << "," << _word;
    const LiteTerm* liteTerm = GetLiteTerm();
    if (liteTerm) {
        ss << " | LiteTerm:" << liteTerm->GetIndexId() << "," << liteTerm->GetTruncateIndexId() << ","
           << liteTerm->GetTermHashKey();
    }
    if (_isNull) {
        ss << " | isNull:true";
    }
    if (HasHintValues()) {
        ss << " |hintValues:" << _hintValues;
    }
    ss << "]";
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, const Term& term) { return os << term.ToString(); }

void Term::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("word", _word);
    json.Jsonize("indexName", _indexName);
    json.Jsonize("truncateName", _truncateName);

    if (json.GetMode() == TO_JSON) {
        if (_isNull) {
            json.Jsonize("isNull", _isNull);
        }
        if (HasHintValues()) {
            json.Jsonize("hintValues", _hintValues);
        }
    } else {
        json.Jsonize("hintValues", _hintValues, _hintValues);
    }
}
} // namespace indexlib::index
