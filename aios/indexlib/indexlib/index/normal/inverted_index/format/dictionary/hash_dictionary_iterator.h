#ifndef __INDEXLIB_HASH_DICTIONARY_ITERATOR_H
#define __INDEXLIB_HASH_DICTIONARY_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_iterator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class HashDictionaryIteratorTyped : public DictionaryIterator
{
private:
    using HashItem = HashDictionaryItemTyped<KeyType>;
    
public: 
    HashDictionaryIteratorTyped(HashItem* dictItemData, uint32_t dictItemCount);
    ~HashDictionaryIteratorTyped();

    bool HasNext() const override;
    void Next(dictkey_t& key, dictvalue_t& value) override;
    bool IsValid() const
    {
        return mDataCursor < mDictItemCount;
    }
    void GetCurrent(dictkey_t& key, dictvalue_t& value)
    {
        key = mDictItemData[mDataCursor].key;
        value = mDictItemData[mDataCursor].value;
    }

    void MoveToNext()
    {
        mDataCursor++;
    }

private:
    void DoNext(KeyType& key, dictvalue_t& value);

private:
    HashItem* mDictItemData;
    uint32_t mDictItemCount;
    uint32_t mDataCursor;
    
private:
    IE_LOG_DECLARE();
};

typedef HashDictionaryIteratorTyped<dictkey_t> HashDictionaryIterator;
typedef std::tr1::shared_ptr<HashDictionaryIterator> HashDictionaryIteratorPtr;

////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, HashDictionaryIteratorTyped);

template <typename KeyType>
HashDictionaryIteratorTyped<KeyType>::HashDictionaryIteratorTyped(
        HashItem* dictItemData, uint32_t dictItemCount)
    : mDictItemData(dictItemData)
    , mDictItemCount(dictItemCount)
    , mDataCursor(0)
{
}

template <typename KeyType>
HashDictionaryIteratorTyped<KeyType>::~HashDictionaryIteratorTyped() 
{
}

template <typename KeyType>
inline bool HashDictionaryIteratorTyped<KeyType>::HasNext() const
{
    return mDataCursor < mDictItemCount;
}

template <typename KeyType>        
inline void HashDictionaryIteratorTyped<KeyType>::Next(dictkey_t& key, dictvalue_t& value)
{
    KeyType typedKey;
    DoNext(typedKey, value);
    key = (dictkey_t)typedKey;
}

template <typename KeyType>        
inline void HashDictionaryIteratorTyped<KeyType>::DoNext(KeyType& key, dictvalue_t& value)
{
    assert(HasNext());
    key = mDictItemData[mDataCursor].key;
    value = mDictItemData[mDataCursor].value;
    mDataCursor++;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_HASH_DICTIONARY_ITERATOR_H
