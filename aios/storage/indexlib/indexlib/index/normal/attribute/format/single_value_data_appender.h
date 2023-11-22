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

#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/data_structure/equal_value_compress_dumper.h"
#include "indexlib/index/normal/attribute/format/single_value_attribute_formatter.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SingleValueDataAppender
{
public:
    SingleValueDataAppender(AttributeFormatter* formatter);
    ~SingleValueDataAppender();

public:
    void Init(uint32_t capacity, const file_system::FileWriterPtr& dataFile);

    template <typename T>
    void Append(const T& value, bool isNull)
    {
        assert(mValueBuffer);
        (static_cast<SingleValueAttributeFormatter<T>*>(mFormatter))->Set(mInBufferCount, mValueBuffer, value, isNull);
        ++mInBufferCount;
        ++mValueCount;
    }

    bool BufferFull() const;
    void Flush();

    template <typename T>
    void FlushCompressBuffer(EqualValueCompressDumper<T>* dumper)
    {
        assert(dumper);
        if (mInBufferCount == 0) {
            return;
        }
        dumper->CompressData((T*)mValueBuffer, mInBufferCount);
        mInBufferCount = 0;
    }

    void CloseFile();

    uint32_t GetValueCount() const { return mValueCount; }
    uint32_t GetInBufferCount() const { return mInBufferCount; }
    uint8_t* GetDataBuffer() const { return mValueBuffer; }
    const file_system::FileWriterPtr& GetDataFileWriter() const { return mDataFileWriter; }

private:
    uint32_t mBufferCapacity;
    uint32_t mInBufferCount;
    uint32_t mValueCount;
    uint8_t* mValueBuffer;
    AttributeFormatter* mFormatter;
    file_system::FileWriterPtr mDataFileWriter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleValueDataAppender);
}} // namespace indexlib::index
