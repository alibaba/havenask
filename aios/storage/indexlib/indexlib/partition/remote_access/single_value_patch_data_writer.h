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

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"
#include "indexlib/index/data_structure/equal_value_compress_dumper.h"
#include "indexlib/index/normal/attribute/format/single_value_attribute_formatter.h"
#include "indexlib/index/normal/attribute/format/single_value_data_appender.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/SimplePool.h"

DECLARE_REFERENCE_CLASS(index, AttributeConvertor);

namespace indexlib { namespace partition {

template <typename T>
class SingleValuePatchDataWriter : public AttributePatchDataWriter
{
public:
    SingleValuePatchDataWriter() {}
    ~SingleValuePatchDataWriter()
    {
        mCompressDumper.reset();
        mPool.reset();
    }

public:
    bool Init(const config::AttributeConfigPtr& attrConfig, const config::MergeIOConfig& mergeIOConfig,
              const file_system::DirectoryPtr& directory) override;

    void AppendValue(const autil::StringView& value) override;
    void AppendNullValue() override;
    void Close() override;
    file_system::FileWriterPtr TEST_GetDataFileWriter() override { return mDataFile; }

private:
    void FlushDataBuffer();

private:
    static const uint32_t DEFAULT_RECORD_COUNT = 1024 * 1024;
    typedef index::EqualValueCompressDumper<T> EqualValueCompressDumper;
    DEFINE_SHARED_PTR(EqualValueCompressDumper);

private:
    index::AttributeFormatterPtr mFormatter;
    index::SingleValueDataAppenderPtr mDataAppender;
    common::AttributeConvertorPtr mAttrConvertor;
    file_system::FileWriterPtr mDataFile;
    EqualValueCompressDumperPtr mCompressDumper;
    std::shared_ptr<autil::mem_pool::PoolBase> mPool;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(partition, SingleValuePatchDataWriter);

/////////////////////////////////////////////////////////////////////////////////
template <typename T>
inline bool SingleValuePatchDataWriter<T>::Init(const config::AttributeConfigPtr& attrConfig,
                                                const config::MergeIOConfig& mergeIOConfig,
                                                const file_system::DirectoryPtr& dirPath)
{
    if (index::AttributeCompressInfo::NeedCompressData(attrConfig)) {
        mPool.reset(new util::SimplePool);
        mCompressDumper.reset(new EqualValueCompressDumper(false, mPool.get()));
    }

    mFormatter.reset(new index::SingleValueAttributeFormatter<T>);
    mFormatter->Init(attrConfig->GetCompressType(), attrConfig->SupportNull());

    file_system::WriterOption writerOption;
    writerOption.bufferSize = mergeIOConfig.writeBufferSize;
    writerOption.asyncDump = mergeIOConfig.enableAsyncWrite;
    mDataFile = dirPath->CreateFileWriter(index::ATTRIBUTE_DATA_FILE_NAME, writerOption);
    mDataAppender.reset(new index::SingleValueDataAppender(mFormatter.get()));
    mDataAppender->Init(DEFAULT_RECORD_COUNT, mDataFile);

    mAttrConvertor.reset(common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    if (!mAttrConvertor) {
        IE_LOG(ERROR, "create attribute convertor fail!");
        return false;
    }
    return true;
}

template <typename T>
inline void SingleValuePatchDataWriter<T>::AppendNullValue()
{
    T value = T();
    mDataAppender->Append(value, true);
    if (mDataAppender->BufferFull()) {
        FlushDataBuffer();
    }
}

template <typename T>
inline void SingleValuePatchDataWriter<T>::AppendValue(const autil::StringView& encodeValue)
{
    assert(mAttrConvertor);
    common::AttrValueMeta meta = mAttrConvertor->Decode(encodeValue);
    const autil::StringView& value = meta.data;
    if (value.size() != sizeof(T)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "value length [%u] not match with record size [%lu]",
                             (uint32_t)value.size(), sizeof(T));
    }

    mDataAppender->Append(*(T*)value.data(), false);
    if (mDataAppender->BufferFull()) {
        FlushDataBuffer();
    }
}

template <typename T>
inline void SingleValuePatchDataWriter<T>::Close()
{
    if (!mDataAppender) {
        return;
    }

    FlushDataBuffer();
    if (mCompressDumper && mDataFile) {
        mCompressDumper->Dump(mDataFile);
        mCompressDumper->Reset();
    }

    mDataAppender->CloseFile();
    mDataAppender.reset();
    mDataFile.reset();
}

template <typename T>
inline void SingleValuePatchDataWriter<T>::FlushDataBuffer()
{
    if (!mDataAppender || mDataAppender->GetInBufferCount() == 0) {
        return;
    }

    if (mCompressDumper) {
        mDataAppender->FlushCompressBuffer(mCompressDumper.get());
    } else {
        assert(mDataFile);
        mDataAppender->Flush();
    }
}
}} // namespace indexlib::partition
