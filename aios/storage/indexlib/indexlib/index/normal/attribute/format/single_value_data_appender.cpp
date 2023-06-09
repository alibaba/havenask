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
#include "indexlib/index/normal/attribute/format/single_value_data_appender.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SingleValueDataAppender);

SingleValueDataAppender::SingleValueDataAppender(AttributeFormatter* formatter)
    : mBufferCapacity(0)
    , mInBufferCount(0)
    , mValueCount(0)
    , mValueBuffer(NULL)
    , mFormatter(formatter)
{
    assert(mFormatter);
}

SingleValueDataAppender::~SingleValueDataAppender() { ARRAY_DELETE_AND_SET_NULL(mValueBuffer); }

void SingleValueDataAppender::Init(uint32_t capacity, const FileWriterPtr& dataFile)
{
    mValueCount = 0;
    mInBufferCount = 0;
    mBufferCapacity = capacity;
    mDataFileWriter = dataFile;

    size_t bufferLen = mFormatter->GetDataLen(capacity);
    mValueBuffer = new uint8_t[bufferLen];
}

bool SingleValueDataAppender::BufferFull() const { return mInBufferCount == mBufferCapacity; }

void SingleValueDataAppender::Flush()
{
    if (mInBufferCount == 0) {
        return;
    }
    mDataFileWriter->Write(mValueBuffer, mFormatter->GetDataLen(mInBufferCount)).GetOrThrow();
    mInBufferCount = 0;
}

void SingleValueDataAppender::CloseFile()
{
    if (mDataFileWriter) {
        mDataFileWriter->Close().GetOrThrow();
    }
    mDataFileWriter.reset();
}
}} // namespace indexlib::index
