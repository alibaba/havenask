#include "indexlib/index/normal/inverted_index/truncate/multi_comparator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiComparator);

MultiComparator::MultiComparator() 
{
}

MultiComparator::~MultiComparator() 
{
}

void MultiComparator::AddComparator(const ComparatorPtr& comp)
{
    mComps.push_back(comp);
}

bool MultiComparator::LessThan(const DocInfo* left, 
                               const DocInfo* right) const
{
    for (size_t i = 0; i < mComps.size(); ++i)
    {
        if (mComps[i]->LessThan(left, right))
        {
            return true;
        }
        else if (mComps[i]->LessThan(right, left))
        {
            return false;
        }
    }
    return false;
}


IE_NAMESPACE_END(index);

