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

#include "autil/Log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/Types.h"

namespace indexlib::index {

class DictionaryWriter
{
public:
    DictionaryWriter() : _itemCount(0), _lastKey(0), _nullTermOffset(0), _hasNullTerm(false) {}

    virtual ~DictionaryWriter() {}

    virtual void Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName) = 0;

    virtual void Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
                      size_t itemCount) = 0;

    virtual void AddItem(index::DictKeyInfo key, dictvalue_t offset) = 0;
    virtual void Close() = 0;

    uint32_t GetItemCount() const { return _itemCount; }

    void SetWriterOption(const file_system::WriterOption& option) { _writerOption = option; }

protected:
    template <typename DictKeyType>
    void CheckKeySequence(const index::DictKeyInfo& key);

    void AddNullTermKey(dictvalue_t offset)
    {
        _nullTermOffset = offset;
        _hasNullTerm = true;
    }

    uint32_t _itemCount;
    index::DictKeyInfo _lastKey;
    dictvalue_t _nullTermOffset;
    bool _hasNullTerm;
    file_system::WriterOption _writerOption;

private:
    AUTIL_LOG_DECLARE();
};

template <typename DictKeyType>
void DictionaryWriter::CheckKeySequence(const index::DictKeyInfo& key)
{
    if (_hasNullTerm) {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "should not add key after add null term key:"
                             "currentKey [%s], currentItemCount [%u]",
                             key.ToString().c_str(), _itemCount);
    }

    if (key.IsNull()) {
        return;
    }

    if (_itemCount == 0) {
        return;
    }

    if ((DictKeyType)key.GetKey() > (DictKeyType)_lastKey.GetKey()) {
        return;
    }

    INDEXLIB_FATAL_ERROR(InconsistentState,
                         "should add key by sequence:"
                         "lastKey [%s], currentKey [%s], currentItemCount [%u]",
                         _lastKey.ToString().c_str(), key.ToString().c_str(), _itemCount);
}

} // namespace indexlib::index
