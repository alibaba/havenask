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

#include "autil/Log.h"
#include "indexlib/index/common/Term.h"

namespace indexlib::index {

template <typename T>
class NumberTerm : public Term
{
    typedef T num_type;

public:
    NumberTerm();
    // null Term
    explicit NumberTerm(const std::string& indexName);

    NumberTerm(num_type num, const std::string& indexName);
    NumberTerm(num_type leftNum, bool leftInclusive, num_type rightNum, bool rightInclusive,
               const std::string& indexName);
    ~NumberTerm();

public:
    NumberTerm(const NumberTerm& t);
    NumberTerm& operator=(const NumberTerm& t);
    NumberTerm(NumberTerm&& t);
    NumberTerm& operator=(NumberTerm&& t);

public:
    num_type GetLeftNumber() const { return _leftNum; }
    num_type GetRightNumber() const { return _rightNum; }

    void SetLeftNumber(num_type leftNum)
    {
        _isNull = false;
        _leftNum = leftNum;
    }
    void SetRightNumber(num_type rightNum)
    {
        _isNull = false;
        _rightNum = rightNum;
    }

    bool Validate();

    std::string GetTermName() const override { return "NumberTerm"; }
    bool operator==(const Term& term) const override;
    std::string ToString() const override;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    num_type _leftNum;
    num_type _rightNum;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////

template <typename T>
NumberTerm<T>::NumberTerm() : NumberTerm("")
{
}

template <typename T>
NumberTerm<T>::NumberTerm(const std::string& indexName) : Term(indexName)
{
}

template <typename T>
NumberTerm<T>::NumberTerm(num_type num, const std::string& indexName) : Term("", indexName)
{
    _leftNum = _rightNum = num;
}

template <typename T>
NumberTerm<T>::NumberTerm(num_type leftNum, bool leftInclusive, num_type rightNum, bool rightInclusive,
                          const std::string& indexName)
    : Term("", indexName)
{
    _leftNum = leftNum;
    _rightNum = rightNum;
    if (!leftInclusive) {
        _leftNum++;
    }
    if (!rightInclusive) {
        _rightNum--;
    }
}

template <typename T>
NumberTerm<T>::~NumberTerm()
{
}

template <typename T>
NumberTerm<T>::NumberTerm(const NumberTerm& t) : Term(t)
{
    _leftNum = t._leftNum;
    _rightNum = t._rightNum;
}

template <typename T>
NumberTerm<T>& NumberTerm<T>::operator=(const NumberTerm& t)
{
    if (this != &t) {
        Term::operator=(t);
        _leftNum = t._leftNum;
        _rightNum = t._rightNum;
    }
    return *this;
}

template <typename T>
NumberTerm<T>::NumberTerm(NumberTerm&& t) : Term(std::move(t))
{
    _leftNum = t._leftNum;
    _rightNum = t._rightNum;
}

template <typename T>
NumberTerm<T>& NumberTerm<T>::operator=(NumberTerm&& t)
{
    if (this != &t) {
        Term::operator=(std::move(t));
        _leftNum = t._leftNum;
        _rightNum = t._rightNum;
    }
    return *this;
}

template <typename T>
bool NumberTerm<T>::Validate()
{
    return _leftNum <= _rightNum;
}

template <typename T>
bool NumberTerm<T>::operator==(const Term& term) const
{
    if (&term == this) {
        return true;
    }
    if (term.GetTermName() != GetTermName()) {
        return false;
    }
    const NumberTerm& term2 = dynamic_cast<const NumberTerm&>(term);
    return _leftNum == term2._leftNum && _rightNum == term2._rightNum && _indexName == term2._indexName;
}

template <typename T>
std::string NumberTerm<T>::ToString() const
{
    std::stringstream ss;
    ss << "NumberTerm: {" << _indexName << ",";
    if (_isNull) {
        ss << "[isNull:true]";
    } else {
        ss << "[" << GetLeftNumber() << ", " << GetRightNumber() << "]";
    }
    return ss.str();
}

template <typename T>
void NumberTerm<T>::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("leftNum", _leftNum);
    json.Jsonize("rightNum", _rightNum);
    Term::Jsonize(json);
}

using Int32Term = NumberTerm<int32_t>;
using Int64Term = NumberTerm<int64_t>;
using UInt64Term = NumberTerm<uint64_t>;

} // namespace indexlib::index
