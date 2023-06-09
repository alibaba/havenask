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
#include "indexlib/partition/remote_access/var_num_patch_data_writer.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/data_structure/adaptive_attribute_offset_dumper.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::common;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, VarNumPatchDataWriter);

VarNumPatchDataWriter::VarNumPatchDataWriter() {}

VarNumPatchDataWriter::~VarNumPatchDataWriter() { mDataWriter.reset(); }

bool VarNumPatchDataWriter::Init(const AttributeConfigPtr& attrConfig, const MergeIOConfig& mergeIOConfig,
                                 const file_system::DirectoryPtr& directory)
{
    file_system::WriterOption writerOption;
    writerOption.bufferSize = mergeIOConfig.writeBufferSize;
    writerOption.asyncDump = mergeIOConfig.enableAsyncWrite;
    mDataFile = directory->CreateFileWriter(ATTRIBUTE_DATA_FILE_NAME, writerOption);
    mOffsetFile = directory->CreateFileWriter(ATTRIBUTE_OFFSET_FILE_NAME, writerOption);

    // small file no need async write
    writerOption.asyncDump = false;
    mDataInfoFile = directory->CreateFileWriter(ATTRIBUTE_DATA_INFO_FILE_NAME, writerOption);

    VarLenDataParam param;
    param.equalCompressOffset = AttributeCompressInfo::NeedCompressOffset(attrConfig);
    param.enableAdaptiveOffset = AttributeCompressInfo::NeedEnableAdaptiveOffset(attrConfig);
    param.appendDataItemLength = false;
    param.dataItemUniqEncode = attrConfig->IsUniqEncode();
    param.offsetThreshold = attrConfig->GetU32OffsetThreshold();

    mDataWriter.reset(new VarLenDataWriter(&mPool));
    mDataWriter->Init(mOffsetFile, mDataFile, param);
    mAttrConvertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    if (!mAttrConvertor) {
        IE_LOG(ERROR, "create attribute convertor fail!");
        return false;
    }
    return true;
}

void VarNumPatchDataWriter::AppendNullValue()
{
    assert(mAttrConvertor);
    string nullString = mAttrConvertor->EncodeNullValue();
    AppendValue(StringView(nullString));
}

void VarNumPatchDataWriter::AppendValue(const StringView& encodeValue)
{
    assert(mAttrConvertor);
    assert(mDataWriter);
    common::AttrValueMeta meta = mAttrConvertor->Decode(encodeValue);
    mDataWriter->AppendValue(meta.data, meta.hashKey);
}

void VarNumPatchDataWriter::Close()
{
    assert(mDataWriter);
    mDataWriter->Close();

    AttributeDataInfo dataInfo(mDataWriter->GetDataItemCount(), mDataWriter->GetMaxItemLength());
    string content = dataInfo.ToString();
    mDataInfoFile->Write(content.c_str(), content.length()).GetOrThrow();
    mDataInfoFile->Close().GetOrThrow();
}
}} // namespace indexlib::partition
