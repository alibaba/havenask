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

#include <memory>

#include "indexlib/index/inverted_index/format/dictionary/CommonDiskTieredDictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

template <typename KeyType>
class CommonDiskTieredDictionaryReaderTyped : public DictionaryReader
{
public:
    typedef std::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::shared_ptr<CommonDiskTieredDictionaryIteratorTyped<KeyType>> CommonDiskTieredDictionaryIteratorPtr;

    CommonDiskTieredDictionaryReaderTyped() : _dictDataLength(0), _blockCount(0) {}
    ~CommonDiskTieredDictionaryReaderTyped() {}

    Status Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
                bool supportFileCompress) override;

    index::Result<bool> InnerLookup(dictkey_t key, file_system::ReadOption option,
                                    dictvalue_t& value) noexcept override;

    std::shared_ptr<DictionaryIterator> CreateIterator() const override
    {
        assert(_fileReader);
        CommonDiskTieredDictionaryIteratorPtr iterator;
        if (_hasNullTerm) {
            iterator.reset(new CommonDiskTieredDictionaryIteratorTyped<KeyType>(_fileReader, _dictDataLength, nullptr,
                                                                                _blockCount, _nullTermValue));
        } else {
            iterator.reset(new CommonDiskTieredDictionaryIteratorTyped<KeyType>(_fileReader, _dictDataLength, nullptr,
                                                                                _blockCount));
        }
        return iterator;
    }

private:
    std::shared_ptr<file_system::FileReader> _fileReader;
    size_t _dictDataLength;
    uint32_t _blockCount;

    AUTIL_LOG_DECLARE();
};

typedef CommonDiskTieredDictionaryReaderTyped<dictkey_t> CommonDiskTieredDictionaryReader;
typedef std::shared_ptr<CommonDiskTieredDictionaryReader> CommonDiskTieredDictionaryReaderPtr;

//////////////////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, CommonDiskTieredDictionaryReaderTyped, KeyType);

template <typename KeyType>
Status CommonDiskTieredDictionaryReaderTyped<KeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                                            const std::string& fileName, bool supportFileCompress)
{
    file_system::ReaderOption option(file_system::FSOT_BUFFERED);
    option.supportCompress = supportFileCompress;
    _fileReader = directory->CreateFileReader(fileName, option);

    auto status = ReadBlockMeta<KeyType>(_fileReader, sizeof(KeyType), _blockCount, _dictDataLength);
    RETURN_IF_STATUS_ERROR(status, "read block meta fail");
    assert(_dictDataLength >= 0);
    return _fileReader->Seek(0).Status();
}

template <typename KeyType>
index::Result<bool>
CommonDiskTieredDictionaryReaderTyped<KeyType>::InnerLookup(dictkey_t key, indexlib::file_system::ReadOption option,
                                                            dictvalue_t& value) noexcept
{
    return index::ErrorCode::UnSupported;
}

} // namespace indexlib::index
