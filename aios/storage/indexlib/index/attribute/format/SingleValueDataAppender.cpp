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
#include "indexlib/index/attribute/format/SingleValueDataAppender.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SingleValueDataAppender);

SingleValueDataAppender::SingleValueDataAppender(AttributeFormatter* formatter)
    : _capacity(0)
    , _inBufferCount(0)
    , _totalCount(0)
    , _formatter(formatter)
    , _dataBuffer(NULL)
{
    assert(_formatter != nullptr);
}

void SingleValueDataAppender::Init(uint32_t capacity,
                                   const std::shared_ptr<indexlib::file_system::FileWriter>& dataFile)
{
    _capacity = capacity;
    _dataFileWriter = dataFile;

    size_t bufferLen = _formatter->GetDataLen(capacity);
    _dataBuffer.reset(new uint8_t[bufferLen]);
}

Status SingleValueDataAppender::Flush()
{
    if (_inBufferCount == 0) {
        return Status::OK();
    }
    auto status = _dataFileWriter->Write(_dataBuffer.get(), _formatter->GetDataLen(_inBufferCount)).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "flush data buffer fail in single value data appender");
        return Status::InternalError();
    }
    _inBufferCount = 0;
    return Status::OK();
}

void SingleValueDataAppender::Close()
{
    if (_dataFileWriter) {
        _dataFileWriter->Close().GetOrThrow();
    }
    _dataFileWriter.reset();
}
} // namespace indexlibv2::index
