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
#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_WRITER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_WRITER_H

#include <memory>

#include "autil/LongHashValue.h"
#include "fslib/fs/File.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/common_define.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_creator.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/format/in_mem_single_value_attribute_formatter.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueAttributeWriter : public AttributeWriter
{
public:
    SingleValueAttributeWriter(const config::AttributeConfigPtr& attrConfig)
        : AttributeWriter(attrConfig)
        , mIsDataFileCopyOnDump(false)
    {
        mFormatter.reset(new InMemSingleValueAttributeFormatter<T>(attrConfig));
    }

    ~SingleValueAttributeWriter() {}

    DECLARE_ATTRIBUTE_WRITER_IDENTIFIER(single);

public:
    class Creator : public AttributeWriterCreator
    {
    public:
        FieldType GetAttributeType() const { return common::TypeInfo<T>::GetFieldType(); }

        AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const
        {
            return new SingleValueAttributeWriter<T>(attrConfig);
        }
    };

public:
    void Init(const FSWriterParamDeciderPtr& fsWriterParamDecider,
              util::BuildResourceMetrics* buildResourceMetrics) override;

    void AddField(docid_t docId, const autil::StringView& attributeValue, bool isNull = false) override;

    bool UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull = false) override;

    void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) override;

    inline const AttributeSegmentReaderPtr CreateInMemReader() const override;

    void AddField(docid_t docId, const T& value, bool isNull = false);

    // dump data to dir, no mkdir sub attr name dir
    // user create attr dir
    void DumpData(const file_system::DirectoryPtr& dir)
    {
        util::SimplePool pool;
        // TODO: tempearature
        mFormatter->DumpFile(dir, ATTRIBUTE_DATA_FILE_NAME, mTemperatureLayer, &pool);
    }

private:
    friend class SingleValueAttributeWriterTest;

    void UpdateBuildResourceMetrics() override;

private:
    bool mIsDataFileCopyOnDump;
    std::shared_ptr<InMemSingleValueAttributeFormatter<T>> mFormatter;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeWriter);

/////////////////////////////////////////////////////////
// inline functions

template <typename T>
inline void SingleValueAttributeWriter<T>::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode) {
        return;
    }
    int64_t poolSize = mPool->getUsedBytes() + mSimplePool.getUsedBytes();
    int64_t dumpFileSize = mFormatter->GetDumpFileSize();
    int64_t dumpTempBufferSize = 0;
    if (mAttrConfig->GetCompressType().HasEquivalentCompress()) {
        // compressor need double buffer
        // worst case : no compress at all
        dumpTempBufferSize = dumpFileSize * 2;
    }
    if (mIsDataFileCopyOnDump) {
        dumpTempBufferSize += dumpFileSize;
    }

    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempBufferSize);
    // do not report zero value metrics for performance consideration
    // mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, 0);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

template <typename T>
inline void SingleValueAttributeWriter<T>::Init(const FSWriterParamDeciderPtr& fsWriterParamDecider,
                                                util::BuildResourceMetrics* buildResourceMetrics)
{
    AttributeWriter::Init(fsWriterParamDecider, buildResourceMetrics);
    mFormatter->Init(mFsWriterParamDecider, mPool);
    mIsDataFileCopyOnDump = mFsWriterParamDecider->MakeParam(ATTRIBUTE_DATA_FILE_NAME).copyOnDump;
    mFormatter->SetAttrConvertor(mAttrConvertor);
    UpdateBuildResourceMetrics();
}

template <typename T>
inline void SingleValueAttributeWriter<T>::AddField(docid_t docId, const autil::StringView& attributeValue, bool isNull)
{
    mFormatter->AddField(docId, attributeValue, isNull);
    UpdateBuildResourceMetrics();
}

// add field must inorder and no loss docid
template <typename T>
inline void SingleValueAttributeWriter<T>::AddField(docid_t docId, const T& value, bool isNull)
{
    mFormatter->AddField(docId, value, isNull);
    UpdateBuildResourceMetrics();
}

template <typename T>
inline bool SingleValueAttributeWriter<T>::UpdateField(docid_t docId, const autil::StringView& attributeValue,
                                                       bool isNull)
{
    mFormatter->UpdateField(docId, attributeValue, isNull);
    UpdateBuildResourceMetrics();
    return true;
}

template <typename T>
inline void SingleValueAttributeWriter<T>::Dump(const file_system::DirectoryPtr& directory,
                                                autil::mem_pool::PoolBase* dumpPool)
{
    // TODO: tempearature
    mFormatter->Dump(directory, mTemperatureLayer, dumpPool);
    UpdateBuildResourceMetrics();
}

template <typename T>
inline const AttributeSegmentReaderPtr SingleValueAttributeWriter<T>::CreateInMemReader() const
{
    return AttributeSegmentReaderPtr(new InMemSingleValueAttributeReader<T>(mFormatter.get()));
}

template <>
inline const AttributeSegmentReaderPtr SingleValueAttributeWriter<int16_t>::CreateInMemReader() const
{
    const config::CompressTypeOption& compress = mAttrConfig->GetCompressType();
    if (compress.HasFp16EncodeCompress()) {
        return AttributeSegmentReaderPtr(new InMemSingleValueAttributeReader<float>(mFormatter.get(), compress));
    }
    return AttributeSegmentReaderPtr(new InMemSingleValueAttributeReader<int16_t>(mFormatter.get()));
}

template <>
inline const AttributeSegmentReaderPtr SingleValueAttributeWriter<int8_t>::CreateInMemReader() const
{
    const config::CompressTypeOption& compress = mAttrConfig->GetCompressType();
    if (compress.HasInt8EncodeCompress()) {
        return AttributeSegmentReaderPtr(new InMemSingleValueAttributeReader<float>(mFormatter.get(), compress));
    }
    return AttributeSegmentReaderPtr(new InMemSingleValueAttributeReader<int8_t>(mFormatter.get()));
}

typedef SingleValueAttributeWriter<float> FloatAttributeWriter;
DEFINE_SHARED_PTR(FloatAttributeWriter);

typedef SingleValueAttributeWriter<double> DoubleAttributeWriter;
DEFINE_SHARED_PTR(DoubleAttributeWriter);

typedef SingleValueAttributeWriter<int64_t> Int64AttributeWriter;
DEFINE_SHARED_PTR(Int64AttributeWriter);

typedef SingleValueAttributeWriter<uint64_t> UInt64AttributeWriter;
DEFINE_SHARED_PTR(UInt64AttributeWriter);

typedef SingleValueAttributeWriter<uint64_t> Hash64AttributeWriter;
DEFINE_SHARED_PTR(Hash64AttributeWriter);

typedef SingleValueAttributeWriter<autil::uint128_t> UInt128AttributeWriter;
DEFINE_SHARED_PTR(UInt128AttributeWriter);

typedef SingleValueAttributeWriter<autil::uint128_t> Hash128AttributeWriter;
DEFINE_SHARED_PTR(Hash128AttributeWriter);

typedef SingleValueAttributeWriter<int32_t> Int32AttributeWriter;
DEFINE_SHARED_PTR(Int32AttributeWriter);

typedef SingleValueAttributeWriter<uint32_t> UInt32AttributeWriter;
DEFINE_SHARED_PTR(UInt32AttributeWriter);

typedef SingleValueAttributeWriter<int16_t> Int16AttributeWriter;
DEFINE_SHARED_PTR(Int16AttributeWriter);

typedef SingleValueAttributeWriter<uint16_t> UInt16AttributeWriter;
DEFINE_SHARED_PTR(UInt16AttributeWriter);

typedef SingleValueAttributeWriter<int8_t> Int8AttributeWriter;
DEFINE_SHARED_PTR(Int8AttributeWriter);

typedef SingleValueAttributeWriter<uint8_t> UInt8AttributeWriter;
DEFINE_SHARED_PTR(UInt8AttributeWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_WRITER_H
