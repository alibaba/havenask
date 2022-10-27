#ifndef __INDEXLIB_DOC_VALUE_FILTER_H
#define __INDEXLIB_DOC_VALUE_FILTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/common/term.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class DocValueFilter
{
public:
    DocValueFilter(autil::mem_pool::Pool *pool)
        : mSessionPool(pool)
    {}
    virtual ~DocValueFilter() {}
public:
    virtual bool Test(docid_t docid) = 0;
    virtual DocValueFilter* Clone() const = 0;
    autil::mem_pool::Pool* GetSessionPool() const {
        return mSessionPool;
    }
protected:
    autil::mem_pool::Pool* mSessionPool;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocValueFilter);

template<typename T>
class NumberDocValueFilterTyped : public DocValueFilter
{
public:
    typedef SingleValueAttributeReader<T> SingleAttributeReader;
    DEFINE_SHARED_PTR(SingleAttributeReader);
    typedef AttributeIteratorTyped<T> AttributeIterator;
    
    NumberDocValueFilterTyped(const AttributeReaderPtr& attrReader,
                              int64_t left, int64_t right,
                              autil::mem_pool::Pool *sessionPool)
        : DocValueFilter(sessionPool)
        , mLeft(left)
        , mRight(right)
        , mAttrIter(NULL)
    {
        mAttrReader = DYNAMIC_POINTER_CAST(SingleAttributeReader, attrReader);
        if (mAttrReader)
        {
            mAttrIter = mAttrReader->CreateIterator(mSessionPool);
        }
    }
    
    NumberDocValueFilterTyped(const NumberDocValueFilterTyped<T>& other)
        : DocValueFilter(other)
        , mLeft(other.mLeft)
        , mRight(other.mRight)
        , mAttrReader(other.mAttrReader)
        , mAttrIter(NULL)
    {
        if (mAttrReader)
        {
            mAttrIter = mAttrReader->CreateIterator(mSessionPool);
        }
    }

    ~NumberDocValueFilterTyped()
    {
        if (mAttrIter)
        {
            IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mAttrIter);
        }
    }
    
    bool Test(docid_t docid) override { return InnerTest(docid); }
    
    bool InnerTest(docid_t docid)
    {
        T value;
        if (!mAttrIter || !mAttrIter->Seek(docid, value))
        {
            return false;
        }
        return mLeft <= (int64_t)value && (int64_t)value <= mRight;
    }
    
    DocValueFilter* Clone() const override
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(
            mSessionPool, NumberDocValueFilterTyped<T>, *this);
    }
    
private:
    int64_t mLeft;
    int64_t mRight;
    SingleAttributeReaderPtr mAttrReader;
    AttributeIterator* mAttrIter;
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_VALUE_FILTER_H
