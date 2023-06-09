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

#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskHashDictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/HashDictionaryReader.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

template <typename KeyType>
class CommonDiskHashDictionaryReaderTyped : public DictionaryReader
{
public:
    typedef std::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::shared_ptr<CommonDiskHashDictionaryIteratorTyped<KeyType>> CommonDiskHashDictionaryIteratorPtr;

    CommonDiskHashDictionaryReaderTyped() : _dictDataLength(0) {}
    ~CommonDiskHashDictionaryReaderTyped() {}

    Status Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
                bool supportFileCompress) override;

    index::Result<bool> InnerLookup(dictkey_t key, file_system::ReadOption option,
                                    dictvalue_t& value) noexcept override;

    std::shared_ptr<DictionaryIterator> CreateIterator() const override
    {
        assert(_fileReader);
        if (_hasNullTerm) {
            return CommonDiskHashDictionaryIteratorPtr(
                new CommonDiskHashDictionaryIteratorTyped<KeyType>(_fileReader, _dictDataLength, _nullTermValue));
        }
        return CommonDiskHashDictionaryIteratorPtr(
            new CommonDiskHashDictionaryIteratorTyped<KeyType>(_fileReader, _dictDataLength));
    }

private:
    std::shared_ptr<file_system::FileReader> _fileReader;
    size_t _dictDataLength;

    AUTIL_LOG_DECLARE();
};

typedef CommonDiskHashDictionaryReaderTyped<dictkey_t> CommonDiskHashDictionaryReader;
typedef std::shared_ptr<CommonDiskHashDictionaryReader> CommonDiskHashDictionaryReaderPtr;

//////////////////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, CommonDiskHashDictionaryReaderTyped, KeyType);

template <typename KeyType>
Status CommonDiskHashDictionaryReaderTyped<KeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                                          const std::string& fileName, bool supportFileCompress)
{
    _fileReader = directory->CreateFileReader(fileName, file_system::FSOT_BUFFERED);
    uint32_t blockCount = 0;
    auto status = ReadBlockMeta<KeyType>(_fileReader, sizeof(uint32_t), blockCount, _dictDataLength);
    RETURN_IF_STATUS_ERROR(status, "read block meta fail");
    assert(_dictDataLength >= 0);
    return _fileReader->Seek(0).Status();
}

template <typename KeyType>
index::Result<bool> CommonDiskHashDictionaryReaderTyped<KeyType>::InnerLookup(dictkey_t key,
                                                                              file_system::ReadOption option,
                                                                              dictvalue_t& value) noexcept
{
    return index::ErrorCode::UnSupported;
}

} // namespace indexlib::index
