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
#include "indexlib/index/inverted_index/format/dictionary/InMemDictionaryReader.h"

#include "indexlib/index/common/Constant.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InMemDictionaryReader);

InMemDictionaryReader::InMemDictionaryReader(HashKeyVector* hashKeyVectorPtr) : _hashKeyVectorPtr(hashKeyVectorPtr) {}

class InMemDictionaryIterator : public DictionaryIterator
{
public:
    InMemDictionaryIterator(InMemDictionaryReader::HashKeyVector::Iterator iterator)
    {
        while (iterator.has_next()) {
            auto key = iterator.next();
            _keys.push_back(key);
        }
        std::sort(_keys.begin(), _keys.end());
        _iterator = _keys.begin();
    }
    bool HasNext() const override { return _iterator != _keys.end(); }
    void Next(index::DictKeyInfo& key, dictvalue_t& value) override
    {
        key = index::DictKeyInfo(*_iterator);
        value = INVALID_DICT_VALUE;
        _iterator++;
    }

private:
    std::vector<dictkey_t> _keys;
    std::vector<dictkey_t>::iterator _iterator;
};

std::shared_ptr<DictionaryIterator> InMemDictionaryReader::CreateIterator() const
{
    return std::shared_ptr<DictionaryIterator>(new InMemDictionaryIterator(_hashKeyVectorPtr->snapshot()));
}

} // namespace indexlib::index
