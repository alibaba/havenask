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

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <memory>
#include <utility>
#include <vector>


namespace autil {
struct KeyOption
{
    KeyOption(int minChar_, int maxChar_, uint32_t keyLen_) :
        minChar(minChar_), maxChar(maxChar_), keyLen(keyLen_)
    {}

    KeyOption() {}

    int minChar;
    int maxChar;
    uint32_t keyLen;
};
//////////////////////////////////////////////////////////////////////////////////////
static const int MIN_CHAR = 0;
static const int MAX_CHAR = 127;
static const size_t HASH_LEN = 20;
static KeyOption gDefaultKeyOption(MIN_CHAR, MAX_CHAR, HASH_LEN);

///////////////////////////////////////////////////////////////////////////////////////
template<typename KeyType = std::string>
class KeyFinder
{
public:

    KeyFinder(const std::vector<KeyType>& keys) : mKeys(keys), mCacheIndex(-1) {}
    virtual ~KeyFinder(){}
private:

    virtual int InternalFind(const KeyType& key)
    {
        typename std::vector<KeyType>::const_iterator it = upper_bound(mKeys.begin(), mKeys.end(), key);

        return (it - mKeys.begin());
    }

public:

    virtual int Find(const KeyType& key)
    {
        if (mCacheIndex > 0 && mCacheIndex < KeyFinder<KeyType>::mKeys.size())
        {
            if (key >= KeyFinder<KeyType>::mKeys[mCacheIndex - 1]
                && key < KeyFinder<KeyType>::mKeys[mCacheIndex])
            {
                return mCacheIndex;
            }
        }
        else if (mCacheIndex == 0)
        {
            if (key < KeyFinder<KeyType>::mKeys[mCacheIndex])
            {
                return mCacheIndex;
            }
        }

        uint32_t index = KeyFinder<KeyType>::InternalFind(key);
        if (index < KeyFinder<KeyType>::mKeys.size())
        {
            mCacheIndex = index;
        }

        return index;
    }

    virtual int SafeFind(const KeyType& key)
    {
        int index = KeyFinder<KeyType>::InternalFind(key);

        return index;
    }

protected:

    std::vector<KeyType> mKeys;
    uint32_t mCacheIndex;
};

/////////////////////////////////////////////////////////////////////////////////////

template<typename KeyType = std::string>
class KeyRangeManagerBase
{
public:
    typedef std::pair<KeyType, KeyType> KeyRangeType;
    typedef std::shared_ptr<KeyFinder<KeyType> > KeyFinderPtr;

    KeyRangeManagerBase() {}
    virtual ~KeyRangeManagerBase() {}
    virtual bool Init() = 0;

    virtual bool GetKeyRange(uint32_t index, KeyRangeType& keyRange)
    {
        if (index < 0 || index >= mKeyRanges.size()) { return false; }

        keyRange.first = mKeyRanges[index].first;
        keyRange.second = mKeyRanges[index].second;

        return true;
    }

    virtual int GetRangeIndexByKey(const KeyType& key) const
    {
        return KeyRangeManagerBase<KeyType>::mKeyFinder->SafeFind(key);
    }

    virtual void GetAllKeyRanges(std::vector<KeyRangeType>& keyRanges) const
    {
        keyRanges.clear();
        keyRanges = mKeyRanges;
    }

    void PrintAllRanges()
    {
        for(uint32_t i = 0; i < mKeyRanges.size(); ++i)
        {
            std::cout << i << "\t" << mKeyRanges[i].first << "\t" << mKeyRanges[i].second << std::endl;
        }
    }

protected:

    void GetKeyMarks(std::vector<KeyType>& keys)
    {
        keys.clear();
        for(uint32_t i = 0; i < mKeyRanges.size(); ++i)
        {
            keys.push_back(mKeyRanges[i].second);
        }
    }

public:

    std::vector<KeyRangeType> mKeyRanges;//minKey < keys[0] < keys[1] < ... < keys[n - 2] < maxKey
    KeyFinderPtr mKeyFinder;
};

/////////////////////////////////////////////////////////////////////////////////////
class AutoHashKeyRangeManager : public KeyRangeManagerBase<std::string>
{
public:

    AutoHashKeyRangeManager(int partCount, const KeyOption& keyOption = gDefaultKeyOption) :
                            mKeyOption(keyOption), mPartCount(partCount)
    {
        Init();
    }

public:

    /*override*/ virtual bool Init()
    {
        if (mPartCount <= 0) { return false; }

        SplitKeyRange();

        std::vector<std::string> keyMarks;
        KeyRangeManagerBase<std::string>::GetKeyMarks(keyMarks);
        KeyRangeManagerBase<std::string>::mKeyFinder =
            std::shared_ptr<KeyFinder<std::string> >(new KeyFinder<std::string>(keyMarks));

        return true;
    }

    static std::string FormatKey(const std::string& str)
    {
        std::ostringstream os;
        for (uint32_t j = 0; j < str.size(); ++j)
        {
            os << int(str.at(j)) << ",";
        }

        return os.str();
    }

private:
    void SplitKeyRange()
    {
        std::string step;
        int remainder = 0;
        for (uint32_t i = 0; i < mKeyOption.keyLen; ++i)
        {
            int quotient = (int(mKeyOption.maxChar) + remainder * int(mKeyOption.maxChar)) / mPartCount;
            step.append(1, char(quotient));
            remainder = (int(mKeyOption.maxChar) + remainder * int(mKeyOption.maxChar)) % mPartCount;
        }

        mKeyRanges.clear();
        std::string startKey(mKeyOption.keyLen, mKeyOption.minChar);
        std::string endKey = startKey;
        for (int i = 0; i < mPartCount; ++i)
        {
            int overflow = 0;
            for (int j = mKeyOption.keyLen - 1; j >= 0; --j)
            {
                int temp = (int)startKey[j] + (int)step[j] + overflow;
                if (temp > mKeyOption.maxChar)
                {
                    endKey[j] = char(temp - mKeyOption.maxChar);
                    overflow = 1;
                }
                else
                {
                    endKey[j] = (char)temp;
                    overflow = 0;
                }
            }
            if (i < mPartCount - 1)
            {
                mKeyRanges.push_back(std::make_pair(startKey, endKey));
            }
            else
            {
                std::string tmp = std::string(mKeyOption.keyLen, mKeyOption.maxChar);
                mKeyRanges.push_back(std::make_pair(startKey, tmp));
            }
            startKey = endKey;
        }
    }

private:

    KeyOption mKeyOption;
    int mPartCount;
};


}

