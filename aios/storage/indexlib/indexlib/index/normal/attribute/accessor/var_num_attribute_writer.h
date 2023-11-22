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

#include <functional>
#include <memory>
#include <unordered_map>

#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/RadixTree.h"
#include "indexlib/index/common/TypedSliceList.h"
#include "indexlib/index/data_structure/adaptive_attribute_offset_dumper.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"
#include "indexlib/index/data_structure/equal_value_compress_dumper.h"
#include "indexlib/index/data_structure/var_len_data_dumper.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_creator.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_var_num_attribute_reader.h"
#include "indexlib/index/util/file_compress_param_helper.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/HashString.h"

namespace indexlib { namespace index {

template <typename T>
class VarNumAttributeWriter : public AttributeWriter
{
public:
    VarNumAttributeWriter(const config::AttributeConfigPtr& attrConfig);

    ~VarNumAttributeWriter() { IE_POOL_COMPATIBLE_DELETE_CLASS(mPool.get(), mAccessor); }

    DECLARE_ATTRIBUTE_WRITER_IDENTIFIER(var_num);

public:
    class Creator : public AttributeWriterCreator
    {
    public:
        FieldType GetAttributeType() const { return common::TypeInfo<T>::GetFieldType(); }

        AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const
        {
            return new VarNumAttributeWriter<T>(attrConfig);
        }
    };

public:
    void Init(const FSWriterParamDeciderPtr& fsWriterParamDecider,
              util::BuildResourceMetrics* buildResourceMetrics) override;
    void AddField(docid_t docId, const autil::StringView& attributeValue, bool isNull = false) override;
    bool UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull = false) override;

    const AttributeSegmentReaderPtr CreateInMemReader() const override;
    void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) override;

protected:
    uint64_t AppendAttribute(const common::AttrValueMeta& meta);

private:
    void UpdateBuildResourceMetrics() override;
    uint64_t InitDumpEstimateFactor() const;

protected:
    VarLenDataAccessor* mAccessor;
    uint64_t mDumpEstimateFactor;
    bool mIsOffsetFileCopyOnDump;
    std::string mNullValue;

private:
    friend class VarNumAttributeWriterTest;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumAttributeWriter);

//////////////////////////////
template <typename T>
VarNumAttributeWriter<T>::VarNumAttributeWriter(const config::AttributeConfigPtr& attrConfig)
    : AttributeWriter(attrConfig)
    , mAccessor(NULL)
    , mDumpEstimateFactor(0)
    , mIsOffsetFileCopyOnDump(false)
{
}

template <typename T>
inline void VarNumAttributeWriter<T>::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode) {
        return;
    }
    int64_t poolSize = mPool->getUsedBytes() + mSimplePool.getUsedBytes();
    // dump file size = data file size + max offset file size
    size_t docCount = mAccessor->GetDocCount();
    int64_t dumpOffsetFileSize = sizeof(uint64_t) * docCount;
    int64_t dumpFileSize = mAccessor->GetAppendDataSize() + dumpOffsetFileSize;
    int64_t dumpTempBufferSize = mDumpEstimateFactor * docCount;
    if (mIsOffsetFileCopyOnDump) {
        dumpTempBufferSize += dumpOffsetFileSize;
    }
    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempBufferSize);
    // do not report zero value metrics for performance consideration
    // mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, 0);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

template <typename T>
inline void VarNumAttributeWriter<T>::Init(const FSWriterParamDeciderPtr& fsWriterParamDecider,
                                           util::BuildResourceMetrics* buildResourceMetrics)
{
    AttributeWriter::Init(fsWriterParamDecider, buildResourceMetrics);
    assert(mPool);
    char* buffer = (char*)mPool->allocate(sizeof(VarLenDataAccessor));
    mAccessor = new (buffer) VarLenDataAccessor();
    mAccessor->Init(mPool.get(), mAttrConfig->IsUniqEncode());
    mDumpEstimateFactor = InitDumpEstimateFactor();
    mIsOffsetFileCopyOnDump = mFsWriterParamDecider->MakeParam(ATTRIBUTE_OFFSET_FILE_NAME).copyOnDump;
    common::VarNumAttributeConvertor<T> convertor;
    mNullValue = convertor.EncodeNullValue();
    UpdateBuildResourceMetrics();
}

template <typename T>
inline void VarNumAttributeWriter<T>::AddField(docid_t docId, const autil::StringView& attributeValue, bool isNull)
{
    assert(mAttrConvertor);
    common::AttrValueMeta meta =
        isNull ? mAttrConvertor->Decode(autil::StringView(mNullValue)) : mAttrConvertor->Decode(attributeValue);
    mAccessor->AppendValue(meta.data, meta.hashKey);
    UpdateBuildResourceMetrics();
}

template <typename T>
inline bool VarNumAttributeWriter<T>::UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull)
{
    assert(mAttrConvertor);
    common::AttrValueMeta meta =
        isNull ? mAttrConvertor->Decode(autil::StringView(mNullValue)) : mAttrConvertor->Decode(attributeValue);
    bool retFlag = mAccessor->UpdateValue(docId, meta.data, meta.hashKey);
    UpdateBuildResourceMetrics();
    return retFlag;
}

template <typename T>
inline void VarNumAttributeWriter<T>::Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool)
{
    std::string attributeName = mAttrConfig->GetAttrName();
    IE_LOG(DEBUG, "Dumping attribute : [%s]", attributeName.c_str());
    {
        /*
            For alter field custom merge task recover
            To be removed
        */
        if (dir->IsExist(attributeName)) {
            IE_LOG(WARN, "File [%s] already exist, will be removed.", attributeName.c_str());
            dir->RemoveDirectory(attributeName);
        }
    }
    auto param = VarLenDataParamHelper::MakeParamForAttribute(mAttrConfig);
    FileCompressParamHelper::SyncParam(mAttrConfig->GetFileCompressConfig(), mTemperatureLayer, param);
    VarLenDataDumper dumper;
    dumper.Init(mAccessor, param);

    file_system::DirectoryPtr attrDir = dir->MakeDirectory(attributeName);
    assert(mFsWriterParamDecider);
    dumper.Dump(attrDir, ATTRIBUTE_OFFSET_FILE_NAME, ATTRIBUTE_DATA_FILE_NAME, mFsWriterParamDecider, nullptr,
                dumpPool);
    AttributeDataInfo dataInfo(dumper.GetDataItemCount(), dumper.GetMaxItemLength());
    attrDir->Store(ATTRIBUTE_DATA_INFO_FILE_NAME, dataInfo.ToString(), file_system::WriterOption());
    IE_LOG(DEBUG, "Finish Store attribute data info");

    UpdateBuildResourceMetrics();
}

template <typename T>
const AttributeSegmentReaderPtr VarNumAttributeWriter<T>::CreateInMemReader() const
{
    InMemVarNumAttributeReader<T>* pReader = new InMemVarNumAttributeReader<T>(
        mAccessor, mAttrConfig->GetCompressType(), mAttrConfig->GetFieldConfig()->GetFixedMultiValueCount(),
        mAttrConfig->SupportNull());
    return AttributeSegmentReaderPtr(pReader);
}

template <typename T>
uint64_t VarNumAttributeWriter<T>::InitDumpEstimateFactor() const
{
    double factor = 0;
    config::CompressTypeOption compressType = mAttrConfig->GetCompressType();
    if (compressType.HasEquivalentCompress()) {
        // use max offset type
        factor += sizeof(uint64_t) * 2;
    }

    if (compressType.HasUniqEncodeCompress()) {
        // hash map for uniq comress dump
        factor += 2 * sizeof(std::pair<uint64_t, uint64_t>);
    }
    // adaptive offset
    factor += sizeof(uint32_t);
    return factor;
}

typedef VarNumAttributeWriter<int8_t> Int8MultiValueAttributeWriter;
DEFINE_SHARED_PTR(Int8MultiValueAttributeWriter);
typedef VarNumAttributeWriter<uint8_t> UInt8MultiValueAttributeWriter;
DEFINE_SHARED_PTR(UInt8MultiValueAttributeWriter);
typedef VarNumAttributeWriter<int16_t> Int16MultiValueAttributeWriter;
DEFINE_SHARED_PTR(Int16MultiValueAttributeWriter);
typedef VarNumAttributeWriter<uint16_t> UInt16MultiValueAttributeWriter;
DEFINE_SHARED_PTR(UInt16MultiValueAttributeWriter);
typedef VarNumAttributeWriter<int32_t> Int32MultiValueAttributeWriter;
DEFINE_SHARED_PTR(Int32MultiValueAttributeWriter);
typedef VarNumAttributeWriter<uint32_t> UInt32MultiValueAttributeWriter;
DEFINE_SHARED_PTR(UInt32MultiValueAttributeWriter);
typedef VarNumAttributeWriter<int64_t> Int64MultiValueAttributeWriter;
DEFINE_SHARED_PTR(Int64MultiValueAttributeWriter);
typedef VarNumAttributeWriter<uint64_t> UInt64MultiValueAttributeWriter;
DEFINE_SHARED_PTR(UInt64MultiValueAttributeWriter);
typedef VarNumAttributeWriter<float> FloatMultiValueAttributeWriter;
DEFINE_SHARED_PTR(FloatMultiValueAttributeWriter);
typedef VarNumAttributeWriter<double> DoubleMultiValueAttributeWriter;
DEFINE_SHARED_PTR(DoubleMultiValueAttributeWriter);
}} // namespace indexlib::index
