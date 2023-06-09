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
#include "indexlib/index/data_structure/var_len_data_param_helper.h"

#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarLenDataParamHelper);

VarLenDataParamHelper::VarLenDataParamHelper() {}

VarLenDataParamHelper::~VarLenDataParamHelper() {}

VarLenDataParam VarLenDataParamHelper::MakeParamForAttribute(const AttributeConfigPtr& attrConfig)
{
    assert(attrConfig);
    VarLenDataParam param;
    param.equalCompressOffset = AttributeCompressInfo::NeedCompressOffset(attrConfig);
    param.enableAdaptiveOffset = AttributeCompressInfo::NeedEnableAdaptiveOffset(attrConfig);
    param.appendDataItemLength = false;
    param.dataItemUniqEncode = attrConfig->IsUniqEncode();
    param.offsetThreshold = attrConfig->GetU32OffsetThreshold();
    param.disableGuardOffset = false;
    return param;
}

VarLenDataParam VarLenDataParamHelper::MakeParamForSummary(const SummaryGroupConfigPtr& summaryGroupConfig)
{
    assert(summaryGroupConfig);
    CompressTypeOption compType = summaryGroupConfig->GetSummaryGroupDataParam().GetCompressTypeOption();
    VarLenDataParam param;
    param.enableAdaptiveOffset = summaryGroupConfig->HasEnableAdaptiveOffset();
    param.equalCompressOffset = compType.HasEquivalentCompress();
    param.dataItemUniqEncode = compType.HasUniqEncodeCompress();
    param.appendDataItemLength = compType.HasUniqEncodeCompress();
    param.disableGuardOffset = true;

    const std::shared_ptr<FileCompressConfig>& fileCompressConfig =
        summaryGroupConfig->GetSummaryGroupDataParam().GetFileCompressConfig();
    param.dataCompressorName = summaryGroupConfig->GetSummaryGroupDataParam().GetFileCompressor();
    param.dataCompressBufferSize = summaryGroupConfig->GetSummaryGroupDataParam().GetFileCompressBufferSize();

    if (fileCompressConfig != nullptr) {
        param.compressWriterExcludePattern = fileCompressConfig->GetExcludePattern();
        param.dataCompressorParams = fileCompressConfig->GetParameters();
    } else if (!summaryGroupConfig->GetSummaryGroupDataParam().IsCompressOffsetFileEnabled()) {
        param.compressWriterExcludePattern = "_SUMMARY_OFFSET_";
    }
    return param;
}

VarLenDataParam VarLenDataParamHelper::MakeParamForSourceData(const config::SourceGroupConfigPtr& sourceGroupConfig)
{
    assert(sourceGroupConfig);
    CompressTypeOption compType = sourceGroupConfig->GetParameter().GetCompressTypeOption();
    VarLenDataParam param;
    param.enableAdaptiveOffset = true;
    param.equalCompressOffset = compType.HasEquivalentCompress();
    param.dataItemUniqEncode = compType.HasUniqEncodeCompress();
    param.appendDataItemLength = compType.HasUniqEncodeCompress();
    param.disableGuardOffset = false;

    const std::shared_ptr<FileCompressConfig>& fileCompressConfig =
        sourceGroupConfig->GetParameter().GetFileCompressConfig();
    param.dataCompressorName = sourceGroupConfig->GetParameter().GetFileCompressor();
    param.dataCompressBufferSize = sourceGroupConfig->GetParameter().GetFileCompressBufferSize();

    if (fileCompressConfig != nullptr) {
        param.compressWriterExcludePattern = fileCompressConfig->GetExcludePattern();
        param.dataCompressorParams = fileCompressConfig->GetParameters();
    } else if (!sourceGroupConfig->GetParameter().IsCompressOffsetFileEnabled()) {
        param.compressWriterExcludePattern = "_SOURCE_OFFSET_";
    }
    return param;
}

VarLenDataParam VarLenDataParamHelper::MakeParamForSourceMeta()
{
    VarLenDataParam metaParam;
    metaParam.enableAdaptiveOffset = true;
    metaParam.equalCompressOffset = true;
    metaParam.dataItemUniqEncode = true;
    metaParam.appendDataItemLength = true;
    metaParam.disableGuardOffset = false;
    return metaParam;
}
}} // namespace indexlib::index
