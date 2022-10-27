#ifndef __INDEXLIB_TIERED_DICTIONARY_ITERATOR_H
#define __INDEXLIB_TIERED_DICTIONARY_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_iterator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class TieredDictionaryIteratorTyped : public DictionaryIterator
{
private:
    typedef DictionaryItemTyped<KeyType> DictionaryItem;
    class DictionaryCompare
    {
    public:
        bool operator () (const DictionaryItem& left, const KeyType& rightKey)
        {
            return left.first < rightKey;
        }

        bool operator () (const KeyType& leftKey, const DictionaryItem& right)
        {
            return leftKey < right.first;
        }
    };
public: 
    TieredDictionaryIteratorTyped(KeyType* blockIndex,
                                  uint32_t blockCount,
                                  DictionaryItem* mDictItemData,
                                  uint32_t dictItemCount);
    ~TieredDictionaryIteratorTyped();

    bool HasNext() const override;
    void Next(dictkey_t& key, dictvalue_t& value) override;
    
    bool Seek(dictkey_t key);
    bool IsValid() const
    {
        return mDataCursor < mDictItemCount;
    }
    void GetCurrent(dictkey_t& key, dictvalue_t& value)
    {
        key = mDictItemData[mDataCursor].first;
        value = mDictItemData[mDataCursor].second;
    }

    void MoveToNext()
    {
        mDataCursor++;
    }

private:
    void DoNext(KeyType& key, dictvalue_t& value);

private:
    DictionaryItem* mDictItemData;
    KeyType* mBlockIndex;
    uint32_t mDictItemCount;
    uint32_t mBlockCount;
    uint32_t mDataCursor;
    
private:
    IE_LOG_DECLARE();
};

typedef TieredDictionaryIteratorTyped<dictkey_t> TieredDictionaryIterator;
typedef std::tr1::shared_ptr<TieredDictionaryIterator> TieredDictionaryIteratorPtr;

////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, TieredDictionaryIteratorTyped);

template <typename KeyType>
TieredDictionaryIteratorTyped<KeyType>::TieredDictionaryIteratorTyped(
        KeyType* blockIndex,
        uint32_t blockCount,
        DictionaryItem* dictItemData,
        uint32_t dictItemCount)
    : mDictItemData(dictItemData)
    , mBlockIndex(blockIndex)
    , mDictItemCount(dictItemCount)
    , mBlockCount(blockCount)
    , mDataCursor(0)
{
}

template <typename KeyType>
TieredDictionaryIteratorTyped<KeyType>::~TieredDictionaryIteratorTyped() 
{
}

template <typename KeyType>
inline bool TieredDictionaryIteratorTyped<KeyType>::HasNext() const
{
    return mDataCursor < mDictItemCount;
}

template <typename KeyType>        
inline void TieredDictionaryIteratorTyped<KeyType>::Next(dictkey_t& key, dictvalue_t& value)
{
    KeyType typedKey;
    DoNext(typedKey, value);
    key = (dictkey_t)typedKey;
}

template <typename KeyType>        
inline void TieredDictionaryIteratorTyped<KeyType>::DoNext(KeyType& key, dictvalue_t& value)
{
    assert(HasNext());
    key = mDictItemData[mDataCursor].first;
    value = mDictItemData[mDataCursor].second;
    mDataCursor++;
}

template <typename KeyType>        
bool TieredDictionaryIteratorTyped<KeyType>::Seek(dictkey_t key)
{
    KeyType* data = std::lower_bound(mBlockIndex, mBlockIndex + mBlockCount, key);
    if (data < mBlockIndex + mBlockCount)
    {
        uint32_t blockCursor  = data - mBlockIndex;
        DictionaryItem* blockStart = mDictItemData + 
                                     blockCursor * ITEM_COUNT_PER_BLOCK;
        DictionaryItem* blockEnd = blockStart + ITEM_COUNT_PER_BLOCK;
        if (blockCursor == mBlockCount - 1)
        {
            blockEnd = (DictionaryItem*)(mBlockIndex);
        }
        DictionaryCompare comp;
        DictionaryItem* dictItem = std::lower_bound(blockStart, blockEnd, key, comp);
        if (dictItem == blockEnd)
        {
            assert(false);//TODO: add ut
            return false;
        }
        
        mDataCursor = dictItem - mDictItemData;
        return true;

    }
    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TIERED_DICTIONARY_ITERATOR_H
