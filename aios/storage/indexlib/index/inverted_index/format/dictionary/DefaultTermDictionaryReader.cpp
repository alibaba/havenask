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
#include "indexlib/index/inverted_index/format/dictionary/DefaultTermDictionaryReader.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/inverted_index/format/DictInlineEncoder.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, DefaultTermDictionaryReader);

Status DefaultTermDictionaryReader::Open(const std::shared_ptr<file_system::Directory>&, const std::string&, bool)
{
    if (_df <= 0) {
        return Status::OK();
    }
    auto iter = _indexConfig->CreateIterator();
    while (iter.HasNext()) {
        auto fieldConfig = iter.Next();
        assert(!fieldConfig->IsEnableNullField());

        const std::string& defaultValue = fieldConfig->GetDefaultValue();
        DictHasher<index::DictFieldType> hasher(fieldConfig->GetUserDefinedParam(), fieldConfig->GetFieldType());
        dictkey_t key;
        bool ret = hasher.CalcHashKey(defaultValue, key);
        if (!ret) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "calculate hash key for default value [%s] in field [%s]",
                                   defaultValue.c_str(), fieldConfig->GetFieldName().c_str());
        }
        _keys.insert(key);
    }

    std::pair<bool, uint64_t> defaultPostingValue;
    [[maybe_unused]] bool ret = DictInlineEncoder::EncodeContinuousDocId(0, _df, true, defaultPostingValue);
    assert(ret);
    auto [dfFirst, inlinePostingValue] = defaultPostingValue;
    _defaultValue = ShortListOptimizeUtil::CreateDictInlineValue(inlinePostingValue, true, dfFirst);
    return Status::OK();
}

namespace {
class DefaultTermDictionaryIter : public DictionaryIterator
{
public:
    DefaultTermDictionaryIter(const std::set<dictkey_t>& keys, dictvalue_t defaultValue /*, DictInfo nullDictInfo*/)
        : _cur(keys.cbegin())
        , _end(keys.cend())
        , _defaultValue(defaultValue)
    {
    }

    ~DefaultTermDictionaryIter() = default;
    bool HasNext() const override { return _cur != _end; }

    void Next(index::DictKeyInfo& key, dictvalue_t& value) override
    {
        key = index::DictKeyInfo(*_cur);
        value = _defaultValue;
        ++_cur;
    }

    void Seek(dictkey_t key) override
    {
        while (*_cur < key && _cur != _end) {
            ++_cur;
        }
    }

    future_lite::coro::Lazy<index::ErrorCode> SeekAsync(dictkey_t key, file_system::ReadOption) noexcept override
    {
        Seek(key);
        co_return index::ErrorCode::OK;
    }

    future_lite::coro::Lazy<index::ErrorCode> NextAsync(index::DictKeyInfo& key, file_system::ReadOption,
                                                        dictvalue_t& value) noexcept override
    {
        Next(key, value);
        co_return index::ErrorCode::OK;
    }

private:
    std::set<dictkey_t>::const_iterator _cur;
    std::set<dictkey_t>::const_iterator _end;
    dictvalue_t _defaultValue;
};

} // namespace

std::shared_ptr<DictionaryIterator> DefaultTermDictionaryReader::CreateIterator() const
{
    return std::make_shared<DefaultTermDictionaryIter>(_keys, _defaultValue);
}

index::Result<bool> DefaultTermDictionaryReader::InnerLookup(dictkey_t key, indexlib::file_system::ReadOption option,
                                                             dictvalue_t& value) noexcept
{
    if (_keys.find(key) == _keys.end()) {
        return false;
    }
    value = _defaultValue;
    return true;
}

} // namespace indexlib::index
