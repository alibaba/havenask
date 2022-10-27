#ifndef __INDEXLIB_COMPARATOR_H
#define __INDEXLIB_COMPARATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info.h"
#include "indexlib/index/normal/inverted_index/truncate/reference.h"
#include "indexlib/index/normal/inverted_index/truncate/reference_typed.h"

IE_NAMESPACE_BEGIN(index);

class Comparator
{
public:
    Comparator() {}
    virtual ~Comparator() {}

public:
    bool operator()(const DocInfo* left, 
                    const DocInfo* right)
    {
        return LessThan(left, right);
    }

    virtual bool LessThan(const DocInfo* left, 
                          const DocInfo* right) const = 0;    
};

template <typename T>
class ComparatorTyped : public Comparator
{
public:
    ComparatorTyped(Reference* refer, bool desc)
    {
        mRef = dynamic_cast<ReferenceTyped<T>* >(refer);
        assert(mRef);
        mDesc = desc;
    }

    ~ComparatorTyped() {}

public:
    virtual bool LessThan(const DocInfo* left, const DocInfo* right) const
    {
        T t1, t2;
        mRef->Get(left, t1);
        mRef->Get(right, t2);
        
        if (!mDesc)
        {
            return t1 < t2;
        }
        else
        {
            return t1 > t2;
        }
    }

private:
    ReferenceTyped<T>* mRef;
    bool mDesc;
};

DEFINE_SHARED_PTR(Comparator);

struct DocInfoComparator
{
public:
    DocInfoComparator() {}
    DocInfoComparator(const ComparatorPtr& comparator)
        : mComparator(comparator)
    {}

    bool operator() (const DocInfo *left, const DocInfo *right)
    {
        return mComparator->LessThan(left, right);
    }

private:
    ComparatorPtr mComparator;
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_COMPARATOR_H
