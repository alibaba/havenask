#ifndef __INDEXLIB_NUMBER_TERM_H
#define __INDEXLIB_NUMBER_TERM_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/common/term.h"
#include <tr1/memory>
#include <sstream>

IE_NAMESPACE_BEGIN(common);

template<typename T>
class NumberTerm : public Term
{
    typedef T num_type;
public:
    NumberTerm(num_type num, const std::string& indexName);
    NumberTerm(num_type leftNum, bool leftInclusive, num_type rightNum,
               bool rightInclusive, const std::string& indexName);
    ~NumberTerm();

public:
    num_type GetLeftNumber() const {return mLeftNum;}
    num_type GetRightNumber() const {return mRightNum;}

    void SetLeftNumber(num_type leftNum) { mLeftNum = leftNum;}
    void SetRightNumber(num_type rightNum) {mRightNum = rightNum;}

    bool Validate();

    std::string GetTermName() const {return "NumberTerm";}
    bool operator == (const Term& term) const;
    std::string ToString() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

private:
    num_type mLeftNum;
    num_type mRightNum;

private:
    IE_LOG_DECLARE();
};


/////////////////////////////////////////////////////////////
///
template<typename T>
NumberTerm<T>::NumberTerm(num_type num, const std::string& indexName) 
  : Term("", indexName) 
{
    mLeftNum = mRightNum = num;
}

template<typename T>
NumberTerm<T>::NumberTerm(num_type leftNum, bool leftInclusive, num_type rightNum,
                       bool rightInclusive, const std::string& indexName) 
    : Term("", indexName)
{
    mLeftNum = leftNum;
    mRightNum = rightNum;
    if (!leftInclusive) {
        mLeftNum++;
    }
    if (!rightInclusive) {
        mRightNum--;
    }
}

template<typename T>
NumberTerm<T>::~NumberTerm() { 
}

template<typename T>
bool NumberTerm<T>::Validate() {
    return mLeftNum <= mRightNum;
}

template<typename T>
bool NumberTerm<T>::operator == (const Term& term) const {
    if (&term == this) {
        return true;
    }
    if (term.GetTermName() != GetTermName()) {
        return false;
    }   
    const NumberTerm &term2 = dynamic_cast<const NumberTerm&>(term);
    return mLeftNum == term2.mLeftNum && 
        mRightNum == term2.mRightNum &&
        mIndexName == term2.mIndexName;
}

template<typename T>
std::string NumberTerm<T>::ToString() const {
    std::stringstream ss;
    ss << "NumberTerm: {" << mIndexName << ",";
    ss << "[" << GetLeftNumber() << ", "
       << GetRightNumber() << "]";
    return ss.str();
}

template<typename T>
void NumberTerm<T>::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("leftNum", mLeftNum);
    json.Jsonize("rightNum", mRightNum);
    Term::Jsonize(json);
}

typedef NumberTerm<int32_t> Int32Term;
typedef std::tr1::shared_ptr<Int32Term> Int32TermPtr;

typedef NumberTerm<int64_t> Int64Term;
typedef std::tr1::shared_ptr<Int64Term> Int64TermPtr;

typedef NumberTerm<uint64_t> UInt64Term;
typedef std::tr1::shared_ptr<UInt64Term> UInt64TermPtr;

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_NUMBER_TERM_H
