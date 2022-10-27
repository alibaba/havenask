#ifndef __INDEXLIB_MULTI_COMPARATOR_H
#define __INDEXLIB_MULTI_COMPARATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/comparator.h"

IE_NAMESPACE_BEGIN(index);

class MultiComparator : public Comparator
{
public:
    MultiComparator();
    ~MultiComparator();
public:
    void AddComparator(const ComparatorPtr& cmp);

    bool LessThan(const DocInfo* left, 
                  const DocInfo* right) const;    

private:
    typedef std::vector<ComparatorPtr> ComparatorVector;
    ComparatorVector mComps;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiComparator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_COMPARATOR_H
