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
#include <utility>

#include "autil/Log.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/UtilDefine.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

class DictionaryReader
{
public:
    typedef std::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;

    using LookupResult = std::pair<bool, dictvalue_t>;

    DictionaryReader() : _hasNullTerm(false), _nullTermValue(0) {}
    virtual ~DictionaryReader() = default;

    /*
     * Open the dictionary file with fileName
     */
    virtual Status Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
                        bool supportFileCompress) = 0;
    /*
     * lookup a key in the dictionary
     * return true if lookup successfully, and return the responding value
     * return false if lookup failed
     */
    index::Result<bool> Lookup(const index::DictKeyInfo& key, file_system::ReadOption option,
                               dictvalue_t& value) noexcept
    {
        if (key.IsNull()) {
            value = _hasNullTerm ? _nullTermValue : 0;
            return _hasNullTerm;
        }
        return InnerLookup(key.GetKey(), option, value);
    }

    future_lite::coro::Lazy<index::Result<LookupResult>> LookupAsync(const index::DictKeyInfo& key,
                                                                     file_system::ReadOption option) noexcept
    {
        if (key.IsNull()) {
            co_return std::make_pair(_hasNullTerm, _hasNullTerm ? _nullTermValue : 0);
        }
        co_return co_await InnerLookupAsync(key.GetKey(), option);
    }

    virtual std::shared_ptr<DictionaryIterator> CreateIterator() const = 0;
    // InnerLookup lookup term except null
    virtual index::Result<bool> InnerLookup(dictkey_t key, file_system::ReadOption option,
                                            dictvalue_t& value) noexcept = 0;

    virtual future_lite::coro::Lazy<index::Result<LookupResult>>
    InnerLookupAsync(dictkey_t key, file_system::ReadOption option) noexcept
    {
        dictvalue_t value;
        auto ret = InnerLookup(key, option, value);
        if (!ret.Ok()) {
            co_return ret.GetErrorCode();
        }
        co_return std::make_pair(ret.Value(), value);
    }

    virtual void EnableBloomFilter(uint32_t multipleNum, uint32_t hashFuncNum) {}
    virtual size_t EstimateMemUsed() const { return 0; }

protected:
    template <typename KeyType>
    Status ReadBlockMeta(const std::shared_ptr<file_system::FileReader>& fileReader, size_t blockUnitSize,
                         uint32_t& blockCount, size_t& dictDataLen);

    bool _hasNullTerm;
    dictvalue_t _nullTermValue;

private:
    AUTIL_LOG_DECLARE();
};

template <typename KeyType>
Status DictionaryReader::ReadBlockMeta(const std::shared_ptr<file_system::FileReader>& fileReader, size_t blockUnitSize,
                                       uint32_t& blockCount, size_t& dictDataLen)
{
    assert(fileReader);
    size_t fileLength = (size_t)fileReader->GetLogicLength();
    if (fileLength < 2 * sizeof(uint32_t)) {
        RETURN_IF_STATUS_ERROR(Status::IOError(),
                               "Bad dictionary file, fileLength[%zd]"
                               " less than 2*sizeof(uint32_t), filePath is: %s",
                               fileLength, fileReader->DebugString().c_str());
    }

    uint32_t magicNum = 0;
    size_t cursor = fileLength - sizeof(uint32_t);
    size_t ret = fileReader->Read((void*)&magicNum, sizeof(uint32_t), cursor).GetOrThrow();
    if (ret != sizeof(uint32_t)) {
        RETURN_IF_STATUS_ERROR(Status::IOError(),
                               "Bad dictionary file, read magic number in file [%s] offset [%lu] fail.",
                               fileReader->DebugString().c_str(), cursor);
    }

    if (magicNum != DICTIONARY_MAGIC_NUMBER && magicNum != DICTIONARY_WITH_NULLTERM_MAGIC_NUMBER) {
        RETURN_IF_STATUS_ERROR(Status::IOError(),
                               "Bad dictionary file, magic number [%x] at offset [%lu] doesn't match, "
                               "expect [%x] or [%x], file name: [%s]",
                               magicNum, cursor, DICTIONARY_MAGIC_NUMBER, DICTIONARY_WITH_NULLTERM_MAGIC_NUMBER,
                               fileReader->DebugString().c_str());
    }
    _hasNullTerm = (magicNum == DICTIONARY_WITH_NULLTERM_MAGIC_NUMBER);
    if (_hasNullTerm) {
        if (cursor < sizeof(dictvalue_t)) {
            RETURN_IF_STATUS_ERROR(Status::IOError(), "invalid cursor[%lu]", cursor);
        }
        auto [status, readLen] =
            fileReader->Read((void*)&_nullTermValue, sizeof(dictvalue_t), cursor - sizeof(dictvalue_t)).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "read null term value from dictionary file [%s] fail.",
                               fileReader->DebugString().c_str());
        if (readLen != sizeof(dictvalue_t)) {
            RETURN_IF_STATUS_ERROR(Status::IOError(), "read null term value from dictionary file [%s] fail.",
                                   fileReader->DebugString().c_str());
        }
        cursor = cursor - sizeof(dictvalue_t);
    }

    if (cursor < sizeof(uint32_t)) {
        RETURN_IF_STATUS_ERROR(Status::IOError(), "invalid cursor[%lu]", cursor);
    }
    auto [status, readLen] =
        fileReader->Read((void*)&blockCount, sizeof(uint32_t), cursor - sizeof(uint32_t)).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "read blockCount from dictionary file [%s] fail.",
                           fileReader->DebugString().c_str());
    if (readLen != sizeof(uint32_t)) {
        RETURN_IF_STATUS_ERROR(Status::IOError(), "read blockCount from dictionary file [%s] fail.",
                               fileReader->DebugString().c_str());
    }
    dictDataLen = fileLength - blockUnitSize * blockCount - sizeof(uint32_t) * 2;
    if (_hasNullTerm) {
        dictDataLen -= sizeof(dictvalue_t);
    }
    return Status::OK();
}

} // namespace indexlib::index
