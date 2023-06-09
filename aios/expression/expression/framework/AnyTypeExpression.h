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
#ifndef ISEARCH_EXPRESSION_ANYTYPEEXPRESSION_H
#define ISEARCH_EXPRESSION_ANYTYPEEXPRESSION_H

#include "expression/common.h"
#include "expression/framework/AttributeExpression.h"
#include "expression/framework/TypeTraits.h"
#include "autil/MultiValueType.h"

namespace expression {

class AnyTypeExpression : public AttributeExpression
{
public:
    AnyTypeExpression(const std::string &exprName);
    ~AnyTypeExpression();
private:
    AnyTypeExpression(const AnyTypeExpression &);
    AnyTypeExpression& operator=(const AnyTypeExpression &);
public:
    /* override */ bool allocate(matchdoc::MatchDocAllocator *allocator,
                                 const std::string &groupName = matchdoc::DEFAULT_GROUP,
                                 uint8_t serializeLevel = SL_PROXY);
    /* override */ bool needEvaluate() const { return false; }    
    /* override */ void evaluate(const matchdoc::MatchDoc &matchDoc);
    /* override */ void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount);
    /* override */ void reset();
    /* override */ matchdoc::ReferenceBase* getReferenceBase() const;
public:
    template <typename T>
    bool getValue(T &value,
                  matchdoc::MatchDoc matchDoc = matchdoc::INVALID_MATCHDOC) const;

    template <typename T>
    bool getValue(autil::MultiValueType<T> &value,
                  matchdoc::MatchDoc matchDoc = matchdoc::INVALID_MATCHDOC) const;

    bool getValue(autil::MultiChar &value,
                  matchdoc::MatchDoc matchDoc = matchdoc::INVALID_MATCHDOC) const;

    template <typename T>
    void setValue(const T &value);

    template <typename T>
    void setValue(const autil::MultiValueType<T> &value);

    void setValue(const autil::MultiChar &value);
private:
    const char *_data;
    uint32_t _len;
private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
inline void AnyTypeExpression::setValue(const T &value) {
    _isMulti = false;
    _exprValueType = AttrValueType2ExprValueType<T>::evt;
    _data = (const char *)&value;
    _len = sizeof(T);
}

template <typename T>
inline void AnyTypeExpression::setValue(const autil::MultiValueType<T> &value) {
    _isMulti = true;
    _exprValueType = AttrValueType2ExprValueType<T>::evt;

    _data = (const char *)value.getData();
    _len = value.size();
}

inline void AnyTypeExpression::setValue(const autil::MultiChar &value) {
    _isMulti = false;
    _exprValueType = EVT_STRING;
    _data = (const char *)value.getData();
    _len = value.size();
}

inline bool AnyTypeExpression::getValue(autil::MultiChar &value,
                                        matchdoc::MatchDoc matchDoc) const
{
    using namespace matchdoc;
    using namespace autil;  
    if (_isMulti || _exprValueType != EVT_STRING) {
        return false;
    }
    
    if (_data) {
        value.init(_data, _len);
        return true;
    }
    return false;
}

template <typename T>
inline bool AnyTypeExpression::getValue(autil::MultiValueType<T> &value,
                                        matchdoc::MatchDoc matchDoc) const
{
    using namespace matchdoc;
    using namespace autil;
    if (!_isMulti || _exprValueType != AttrValueType2ExprValueType<T>::evt ) {
        return false;
    }

    if (_data) {
        value.init(_data, _len);
        return true;
    }
    return false;    
}

template <typename T>
inline bool AnyTypeExpression::getValue(T &value,
                                        matchdoc::MatchDoc matchDoc) const
{
    using namespace matchdoc;
    using namespace autil;
    if (_isMulti || _exprValueType != AttrValueType2ExprValueType<T>::evt) {
        return false;
    }

    if (_data) {
        assert(_len == sizeof(T));
        value = *(const T *)_data;        
        return true;
    }
    return false;
}

}

#endif //ISEARCH_EXPRESSION_ANYTYPEEXPRESSION_H
