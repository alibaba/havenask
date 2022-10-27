#ifndef __INDEXLIB_SORTED_PRIMARY_KEY_PAIR_SEGMENT_ITERATOR_H
#define __INDEXLIB_SORTED_PRIMARY_KEY_PAIR_SEGMENT_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_pair_segment_iterator.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class SortedPrimaryKeyPairSegmentIterator : public PrimaryKeyPairSegmentIterator<Key>
{
public:
    typedef typename PrimaryKeyPairSegmentIterator<Key>::PKPair PKPair;

public:
    SortedPrimaryKeyPairSegmentIterator()
        : mLength(0)
        , mCursor(0)
        , mIsDone(true)
    {}

    virtual ~SortedPrimaryKeyPairSegmentIterator() {}

public:
    void Init(const file_system::FileReaderPtr& fileReader) override;
    bool HasNext() const override;
    void Next(PKPair& pkPair) override;
    void GetCurrentPKPair(PKPair& pair) const override;
    uint64_t GetPkCount() const override;
    
private:
    file_system::FileReaderPtr mFileReader;
    PKPair mCurrentPKPair;
    size_t mLength;
    size_t mCursor;
    bool mIsDone;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SortedPrimaryKeyPairSegmentIterator);

//////////////////////////////////////////////////////////////////////
template<typename Key>
void SortedPrimaryKeyPairSegmentIterator<Key>::Init(
        const file_system::FileReaderPtr& fileReader)
{
    assert(fileReader);
    mFileReader = fileReader;
    mLength = fileReader->GetLength();
    mCursor = 0;
    mIsDone = false;
    Next(mCurrentPKPair);
}

template<typename Key>
bool SortedPrimaryKeyPairSegmentIterator<Key>::HasNext() const
{
    return !mIsDone;
}    

template<typename Key>
void SortedPrimaryKeyPairSegmentIterator<Key>::Next(PKPair& pkPair)
{
    assert(HasNext());
    GetCurrentPKPair(pkPair);

    if (mCursor >= mLength)
    {
        mIsDone = true;
        return;
    }

    size_t readLen = mFileReader->Read((void*)(&mCurrentPKPair), sizeof(PKPair), mCursor);
    if (readLen != sizeof(PKPair))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "read pk data file[%s] fail, file collapse!",
                             mFileReader->GetPath().c_str());
    }
    mCursor += readLen;
}

template<typename Key>
void SortedPrimaryKeyPairSegmentIterator<Key>::GetCurrentPKPair(
        PKPair& pair) const
{
    pair = mCurrentPKPair;
}

template<typename Key>
uint64_t SortedPrimaryKeyPairSegmentIterator<Key>::GetPkCount() const
{
    return mFileReader->GetLength() / sizeof(PKPair);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SORTED_PRIMARY_KEY_PAIR_SEGMENT_ITERATOR_H
