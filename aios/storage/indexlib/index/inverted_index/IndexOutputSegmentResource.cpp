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
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"

#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/FileCompressParamHelper.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, IndexOutputSegmentResource);

IndexOutputSegmentResource::~IndexOutputSegmentResource()
{
    if (_normalIndexDataWriter) {
        _normalIndexDataWriter->Reset();
    }
    if (_bitmapIndexDataWriter) {
        _bitmapIndexDataWriter->Reset();
    }
}

void IndexOutputSegmentResource::Reset() { DestroyIndexDataWriters(); }

void IndexOutputSegmentResource::Init(const file_system::DirectoryPtr& mergeDir,
                                      const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                      const file_system::IOConfig& ioConfig, const std::string& temperatureLayerStr,
                                      util::SimplePool* simplePool, bool needCreateBitmapIndex)
{
    _mergeDir = mergeDir;
    _temperatureLayer = temperatureLayerStr;
    CreateNormalIndexDataWriter(indexConfig, ioConfig, simplePool);
    CreateBitmapIndexDataWriter(ioConfig, simplePool, needCreateBitmapIndex);
}

void IndexOutputSegmentResource::Init(
    const file_system::DirectoryPtr& mergeDir,
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, const file_system::IOConfig& ioConfig,
    const std::shared_ptr<indexlibv2::framework::SegmentStatistics>& segmentStatistics, util::SimplePool* simplePool,
    bool needCreateBitmapIndex)
{
    _mergeDir = mergeDir;
    _segmentStatistics = segmentStatistics;
    CreateNormalIndexDataWriter(indexConfig, ioConfig, simplePool);
    CreateBitmapIndexDataWriter(ioConfig, simplePool, needCreateBitmapIndex);
}

void IndexOutputSegmentResource::CreateNormalIndexDataWriter(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, const file_system::IOConfig& IOConfig,
    util::SimplePool* simplePool)
{
    _normalIndexDataWriter.reset(new IndexDataWriter());
    if (indexConfig->GetFileCompressConfigV2()) {
        _normalIndexDataWriter->dictWriter.reset(
            DictionaryCreator::CreateWriter(indexConfig, _segmentStatistics, simplePool));
    } else {
        _normalIndexDataWriter->dictWriter.reset(
            DictionaryCreator::CreateWriter(indexConfig, _temperatureLayer, simplePool));
    }
    if (_normalIndexDataWriter->dictWriter) {
        if (_dictKeyCount > 0) {
            _normalIndexDataWriter->dictWriter->Open(_mergeDir, DICTIONARY_FILE_NAME, _dictKeyCount);
        } else {
            _normalIndexDataWriter->dictWriter->Open(_mergeDir, DICTIONARY_FILE_NAME);
        }
    } else {
        AUTIL_LOG(ERROR, "create normal index data writer fail, path[%s]", _mergeDir->DebugString().c_str());
    }
    file_system::WriterOption writerOption;
    writerOption.bufferSize = IOConfig.writeBufferSize;
    writerOption.asyncDump = IOConfig.enableAsyncWrite;
    if (indexConfig->GetFileCompressConfigV2()) {
        indexlibv2::index::FileCompressParamHelper::SyncParam(indexConfig->GetFileCompressConfigV2(),
                                                              _segmentStatistics, writerOption);
    } else {
        // for lagacy indexlib v1
        indexlibv2::index::FileCompressParamHelper::SyncParam(indexConfig->GetFileCompressConfig(), _temperatureLayer,
                                                              writerOption);
    }
    _normalIndexDataWriter->postingWriter = _mergeDir->CreateFileWriter(POSTING_FILE_NAME, writerOption);
}

void IndexOutputSegmentResource::CreateBitmapIndexDataWriter(const file_system::IOConfig& IOConfig,
                                                             util::SimplePool* simplePool, bool needCreateBitmapIndex)

{
    if (needCreateBitmapIndex) {
        _bitmapIndexDataWriter.reset(new IndexDataWriter());
        _bitmapIndexDataWriter->dictWriter.reset(new TieredDictionaryWriter<dictkey_t>(simplePool));
        _bitmapIndexDataWriter->dictWriter->Open(_mergeDir, BITMAP_DICTIONARY_FILE_NAME);

        file_system::WriterOption writerOption;
        writerOption.bufferSize = IOConfig.writeBufferSize;
        writerOption.asyncDump = IOConfig.enableAsyncWrite;
        file_system::FileWriterPtr postingFileWriter =
            _mergeDir->CreateFileWriter(BITMAP_POSTING_FILE_NAME, writerOption);
        _bitmapIndexDataWriter->postingWriter = postingFileWriter;
    }
}

std::shared_ptr<IndexDataWriter>& IndexOutputSegmentResource::GetIndexDataWriter(SegmentTermInfo::TermIndexMode mode)
{
    if (mode == SegmentTermInfo::TM_BITMAP) {
        return _bitmapIndexDataWriter;
    }
    return _normalIndexDataWriter;
}

} // namespace indexlib::index
