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
#include "indexlib/index/inverted_index/builtin_index/bitmap/OnDiskBitmapIndexIterator.h"

#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingDecoder.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskTieredDictionaryReader.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, OnDiskBitmapIndexIterator);

OnDiskBitmapIndexIterator::OnDiskBitmapIndexIterator(const file_system::DirectoryPtr& indexDirectory)
    : OnDiskIndexIterator(indexDirectory, OPTION_FLAG_ALL)
    , _termMeta(NULL)
    , _bufferSize(DEFAULT_DATA_BUFFER_SIZE)
    , _dataBuffer(_defaultDataBuffer)
    , _bufferLength(0)
{
}

OnDiskBitmapIndexIterator::~OnDiskBitmapIndexIterator()
{
    FreeDataBuffer();
    DELETE_AND_SET_NULL(_termMeta);
}

void OnDiskBitmapIndexIterator::Init()
{
    _dictionaryReader.reset(new CommonDiskTieredDictionaryReader());
    auto status = _dictionaryReader->Open(_indexDirectory, BITMAP_DICTIONARY_FILE_NAME, /*supportFileCompress=*/false);
    THROW_IF_STATUS_ERROR(status);
    _dictionaryIterator = _dictionaryReader->CreateIterator();

    // in the same file
    _docListFile = _indexDirectory->CreateFileReader(BITMAP_POSTING_FILE_NAME, file_system::FSOT_BUFFERED);
    assert(_docListFile);

    _decoder.reset(new BitmapPostingDecoder());
    _termMeta = new TermMeta();
}

bool OnDiskBitmapIndexIterator::HasNext() const { return _dictionaryIterator->HasNext(); }

PostingDecoder* OnDiskBitmapIndexIterator::Next(index::DictKeyInfo& key)
{
    dictvalue_t value;
    _dictionaryIterator->Next(key, value);
    int64_t docListFileCursor = (int64_t)value;

    size_t readed =
        _docListFile->Read((void*)(&_bufferLength), sizeof(uint32_t), docListFileCursor, file_system::ReadOption())
            .GetOrThrow();
    if (readed < sizeof(uint32_t)) {
        INDEXLIB_FATAL_ERROR(FileIO, "Read file[%s] FAILED!", _docListFile->DebugString().c_str());
    }

    docListFileCursor += sizeof(uint32_t);
    if (_bufferLength > _bufferSize) {
        FreeDataBuffer();
        _dataBuffer = new uint8_t[_bufferLength];
        _bufferSize = _bufferLength;
    }

    readed = _docListFile->Read(_dataBuffer, _bufferLength, docListFileCursor, file_system::ReadOption()).GetOrThrow();
    if (readed < _bufferLength) {
        INDEXLIB_FATAL_ERROR(FileIO, "Read file[%s] FAILED!", _docListFile->DebugString().c_str());
    }

    uint8_t* dataCursor = _dataBuffer;
    size_t leftSize = _bufferLength;
    TermMetaLoader tmLoader;
    tmLoader.Load(dataCursor, leftSize, *_termMeta);

    uint32_t bmSize = *(uint32_t*)dataCursor;
    dataCursor += sizeof(uint32_t);

    _decoder->Init(_termMeta, dataCursor, bmSize);
    return _decoder.get();
}

void OnDiskBitmapIndexIterator::FreeDataBuffer()
{
    if (_dataBuffer != _defaultDataBuffer) {
        delete[] _dataBuffer;
        _dataBuffer = NULL;
    }
}

void OnDiskBitmapIndexIterator::Next(OnDiskBitmapIndexIterator::OnDiskBitmapIndexMeta& bitmapMeta)
{
    Next(bitmapMeta.key);
    getBufferInfo(bitmapMeta.size, bitmapMeta.data);
}

void OnDiskBitmapIndexIterator::getBufferInfo(uint32_t& bufferSize, uint8_t*& dataBuffer)
{
    bufferSize = _bufferLength;
    dataBuffer = _dataBuffer;
}
} // namespace indexlib::index
