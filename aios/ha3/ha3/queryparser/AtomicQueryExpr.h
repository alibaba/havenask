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
#pragma once

#include <algorithm>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Term.h"
#include "ha3/queryparser/QueryExpr.h"

namespace isearch {
namespace queryparser {

class AtomicQueryExpr : public QueryExpr {
public:
    AtomicQueryExpr();
    virtual ~AtomicQueryExpr();

public:
    virtual void setIndexName(const std::string &indexName) {
        _indexName = indexName;
    }
    virtual void setLeafIndexName(const std::string &indexName) override {
        setIndexName(indexName);
    }
    const std::string &getIndexName() const {
        return _indexName;
    }
    virtual void setRequiredFields(const common::RequiredFields &requiredFields) {
        _requiredFields.fields.assign(requiredFields.fields.begin(), requiredFields.fields.end());
        _requiredFields.isRequiredAnd = requiredFields.isRequiredAnd;
        sort(_requiredFields.fields.begin(), _requiredFields.fields.end());
    }
    const common::RequiredFields &getRequiredFields() const {
        return _requiredFields;
    }

    void setBoost(int32_t boost) {
        _boost = boost;
    }
    int32_t getBoost() const {
        return _boost;
    }

    void setSecondaryChain(const std::string &secondaryChain) {
        _secondaryChain = secondaryChain;
    }
    std::string getSecondaryChain() const {
        return _secondaryChain;
    }
    const std::string &getText() const {
        return _text;
    }
    void setText(const std::string &text) {
        _text = text;
    }

    virtual common::TermPtr constructSearchTerm() {
        return common::TermPtr(new common::Term(
            _text.c_str(), _indexName.c_str(), _requiredFields, _boost, _secondaryChain));
    }

protected:
    std::string _indexName;
    std::string _secondaryChain;
    common::RequiredFields _requiredFields;
    int32_t _boost;
    std::string _text;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch
