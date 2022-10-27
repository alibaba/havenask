#ifndef __INDEXLIB_KEY_ITERATOR_TYPED_H
#define __INDEXLIB_KEY_ITERATOR_TYPED_H

#include <queue>
#include <tr1/memory>
#include <sstream>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/key_iterator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_reader.h"

IE_NAMESPACE_BEGIN(index);

struct KeyInfo
{
    DictionaryIteratorPtr dictIter;
    dictkey_t key;
};

class KeyComparator
{
public:
    bool operator() (KeyInfo*& lft, KeyInfo*& rht)
    {
        return lft->key > rht->key;
    }
};

template <class IndexReaderType>
class KeyIteratorTyped : public KeyIterator
{
public:
    KeyIteratorTyped(IndexReaderType& reader);
    ~KeyIteratorTyped();

public:
    bool HasNext() const override;
    PostingIterator* NextPosting(std::string& key) override;

private:
    void ClearKeyHeap();

private:
    void Init();

private:
    typedef std::priority_queue<KeyInfo*, std::vector<KeyInfo*>, 
                                KeyComparator> KeyHeap;

private:
    IndexReaderType& mReader;
    KeyHeap mKeyHeap;

private:
    IE_LOG_DECLARE();
};


/////////////////////////////////////////////

template<class IndexReaderType>
KeyIteratorTyped<IndexReaderType>::KeyIteratorTyped(IndexReaderType& reader)
    : mReader(reader)
{
    Init();
}

template<class IndexReaderType>
KeyIteratorTyped<IndexReaderType>::~KeyIteratorTyped() 
{
    ClearKeyHeap();
}

template<class IndexReaderType>
void KeyIteratorTyped<IndexReaderType>::Init()
{
    const std::vector<DictionaryReaderPtr>& dictReaders = mReader.GetDictReaders();
    for (size_t i = 0; i < dictReaders.size(); ++i)
    {
        KeyInfo* keyInfo = new KeyInfo;
        if (!dictReaders[i])
        {
            delete keyInfo;
            continue;
        }
        keyInfo->dictIter = dictReaders[i]->CreateIterator();
        if (!keyInfo->dictIter->HasNext())
        {
            delete keyInfo;
            continue;
        }
        dictvalue_t value;
        keyInfo->dictIter->Next(keyInfo->key, value);
        mKeyHeap.push(keyInfo);
    }
}

template<class IndexReaderType>
void KeyIteratorTyped<IndexReaderType>::ClearKeyHeap()
{
    while (!mKeyHeap.empty())
    {
        KeyInfo* keyInfo = mKeyHeap.top();
        delete keyInfo;
        mKeyHeap.pop();
    }
}

template<class IndexReaderType>
bool KeyIteratorTyped<IndexReaderType>::HasNext() const
{
    return !mKeyHeap.empty();
}

template<class IndexReaderType>
PostingIterator* KeyIteratorTyped<IndexReaderType>::NextPosting(std::string& strKey)
{
    dictkey_t key;
    KeyInfo* keyInfo = mKeyHeap.top();
    mKeyHeap.pop();

    key = keyInfo->key;
    if (!keyInfo->dictIter->HasNext())
    {
        delete keyInfo;
    }
    else
    {
        dictvalue_t value;
        keyInfo->dictIter->Next(keyInfo->key, value);
        mKeyHeap.push(keyInfo);
    }

    std::stringstream ss;
    ss << key;
    ss >> strKey;

    while (!mKeyHeap.empty())
    {
        keyInfo = mKeyHeap.top();
        if (keyInfo->key != key)
        {
            break;
        }
        mKeyHeap.pop();
        if (!keyInfo->dictIter->HasNext())
        {
            delete keyInfo;
        }
        else
        {
            dictvalue_t value;
            keyInfo->dictIter->Next(keyInfo->key, value);
            mKeyHeap.push(keyInfo);
        }
    }

    return mReader.CreateMainPostingIterator(key, 1000, NULL);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_KEY_ITERATOR_H
