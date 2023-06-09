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

#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/index/inverted_index/OnDiskIndexIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingDecoder.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskTieredDictionaryReader.h"

namespace indexlib::index {

class OnDiskBitmapIndexIterator : public OnDiskIndexIterator
{
public:
    struct OnDiskBitmapIndexMeta {
        OnDiskBitmapIndexMeta() : key(0), size(0), data(NULL) {}

        index::DictKeyInfo key;
        uint32_t size;
        uint8_t* data;
    };

public:
    OnDiskBitmapIndexIterator(const file_system::DirectoryPtr& indexDirectory);
    ~OnDiskBitmapIndexIterator();

    // class Creator : public OnDiskIndexIteratorCreator
    // {
    // public:
    //     Creator() {}
    //     OnDiskIndexIterator* Create(
    //             const file_system::DirectoryPtr& indexDirectory) const override
    //     {
    //         return new OnDiskBitmapIndexIterator(indexDirectory);
    //     }
    // };

public:
    void Init() override;
    bool HasNext() const override;
    PostingDecoder* Next(index::DictKeyInfo& key) override;
    void Next(OnDiskBitmapIndexMeta& bitmapMeta);
    size_t GetPostingFileLength() const override { return _docListFile ? _docListFile->GetLogicLength() : 0; }

private:
    void FreeDataBuffer();
    void getBufferInfo(uint32_t& bufferSize, uint8_t*& dataBuffer);

private:
    std::shared_ptr<CommonDiskTieredDictionaryReader> _dictionaryReader;
    std::shared_ptr<DictionaryIterator> _dictionaryIterator;
    file_system::FileReaderPtr _docListFile;
    TermMeta* _termMeta;
    std::shared_ptr<BitmapPostingDecoder> _decoder;

    static const uint32_t FILE_BUFFER_SIZE = 1024 * 256;
    static const uint32_t DEFAULT_DATA_BUFFER_SIZE = 1024 * 256;
    uint32_t _bufferSize;
    uint8_t _defaultDataBuffer[DEFAULT_DATA_BUFFER_SIZE];
    uint8_t* _dataBuffer;
    uint32_t _bufferLength;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
