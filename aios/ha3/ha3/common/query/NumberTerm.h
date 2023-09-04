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

#include <memory>
#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Term.h"
#include "ha3/isearch.h"

namespace autil {
class DataBuffer;
} // namespace autil

namespace isearch {
namespace common {

class NumberTerm : public Term {
public:
    NumberTerm(int64_t num,
               const char *indexName,
               const RequiredFields &requiredFields,
               int64_t boost = DEFAULT_BOOST_VALUE,
               const std::string &truncateName = "");
    NumberTerm(int64_t leftNum,
               bool leftInclusive,
               int64_t rightNum,
               bool rightInclusive,
               const char *indexName,
               const RequiredFields &requiredFields,
               int64_t boost = DEFAULT_BOOST_VALUE,
               const std::string &truncateName = "");
    NumberTerm(const NumberTerm &other);
    ~NumberTerm();

public:
    int64_t getLeftNumber() const {
        return _leftNum;
    }
    int64_t getRightNumber() const {
        return _rightNum;
    }
    virtual TermType getTermType() const {
        return TT_NUMBER;
    }
    virtual std::string getTermName() const {
        return "NumberTerm";
    }
    virtual Term *clone() {
        return new NumberTerm(*this);
    }
    bool operator==(const Term &term) const;
    std::string toString() const;

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

private:
    int64_t _leftNum;
    int64_t _rightNum;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<NumberTerm> NumberTermPtr;

} // namespace common
} // namespace isearch
