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
#include "indexlib/index/orc/OrcLeafReader.h"

#include "autil/Log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/orc/InputStreamImpl.h"
#include "indexlib/index/orc/OrcConfig.h"
#include "indexlib/index/orc/OrcLeafIterator.h"
#include "orc/OrcFile.hh"

#if defined(__clang__) && (__cplusplus >= 202002L)
#include "indexlib/index/orc/AsyncInputStream.h"
#endif

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(index, OrcLeafReader);

OrcLeafReader::OrcLeafReader() {}

OrcLeafReader::~OrcLeafReader() {}

Status OrcLeafReader::Init(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                           const config::OrcConfig* config)
{
    _orcConfig = config;
    _fileReader = fileReader;

    std::unique_ptr<orc::Reader> reader;
    auto s = CreateOrcReader(ReaderOptions(), reader);
    if (!s.IsOK()) {
        return s;
    }

    _meta.totalRowCount = reader->getNumberOfRows();
    _meta.fileTailStr = reader->getSerializedFileTail();
    // TODO: store meta data such as bloom filters

    return Status::OK();
}

Status OrcLeafReader::CreateIterator(const ReaderOptions& opts, std::unique_ptr<IOrcIterator>& orcIter)
{
    std::unique_ptr<orc::Reader> reader;
    auto s = CreateOrcReader(opts, reader);
    if (!s.IsOK()) {
        return s;
    }

    orc::RowReaderOptions rowReaderOpts = _orcConfig->GetRowReaderOptions();
    if (opts.fieldIds.size() > 0) {
        rowReaderOpts.include(opts.fieldIds);
    }
    try {
        auto rowReader = reader->createRowReader(rowReaderOpts);
        auto batchSize = opts.batchSize > 0 ? opts.batchSize : _orcConfig->GetRowGroupSize();
        orcIter = std::make_unique<OrcLeafIterator>(std::move(reader), std::move(rowReader), batchSize);
        return Status::OK();
    } catch (std::exception& e) {
        return Status::InvalidArgs("create row reader failed, exception: ", e.what());
    }
}

Status OrcLeafReader::CreateOrcReader(const ReaderOptions& opts, std::unique_ptr<orc::Reader>& orcReader) const
{
    orc::ReaderOptions orcOpts = _orcConfig->GetReaderOptions();
    if (!_meta.fileTailStr.empty()) {
        orcOpts.setSerializedFileTail(_meta.fileTailStr);
    }
    if (opts.pool != nullptr) {
        orcOpts.setMemoryPool(opts.pool);
    }

    std::unique_ptr<orc::InputStream> inputStream;
    if (!opts.async) {
        inputStream = std::make_unique<InputStreamImpl>(_fileReader);
    } else {
#if defined(__clang__) && (__cplusplus >= 202002L)
        inputStream = std::make_unique<AsyncInputStream>(_fileReader, opts.executor);
#else
        inputStream = std::make_unique<InputStreamImpl>(_fileReader);
#endif
    }
    try {
        orcReader = orc::createReader(std::move(inputStream), orcOpts);
    } catch (std::exception& e) {
        return Status::InternalError("create orc reader failed, exception: ", e.what());
    }
    return Status::OK();
}

size_t OrcLeafReader::EvaluateCurrentMemUsed() const
{
    if (_fileReader && _fileReader->IsMemLock()) {
        return _fileReader->GetLength();
    }
    return 0;
}

} // namespace indexlibv2::index
