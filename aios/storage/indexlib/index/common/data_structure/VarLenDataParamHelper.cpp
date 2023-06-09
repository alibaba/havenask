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
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"

#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"

using namespace indexlib::config;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, VarLenDataParamHelper);

VarLenDataParamHelper::VarLenDataParamHelper() {}

VarLenDataParamHelper::~VarLenDataParamHelper() {}

VarLenDataParam
VarLenDataParamHelper::MakeParamForAttribute(const std::shared_ptr<indexlibv2::config::AttributeConfig>& attrConfig)
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

VarLenDataParam VarLenDataParamHelper::MakeParamForSummary(
    const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig)
{
    assert(summaryGroupConfig);
    auto compType = summaryGroupConfig->GetSummaryGroupDataParam().GetCompressTypeOption();
    VarLenDataParam param;
    param.enableAdaptiveOffset = summaryGroupConfig->HasEnableAdaptiveOffset();
    param.equalCompressOffset = compType.HasEquivalentCompress();
    param.dataItemUniqEncode = compType.HasUniqEncodeCompress();
    param.appendDataItemLength = compType.HasUniqEncodeCompress();
    param.disableGuardOffset = true;

    const auto& fileCompressConfig = summaryGroupConfig->GetSummaryGroupDataParam().GetFileCompressConfig();
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

VarLenDataParam VarLenDataParamHelper::MakeParamForSourceData(
    const std::shared_ptr<indexlib::config::SourceGroupConfig>& sourceGroupConfig)
{
    assert(sourceGroupConfig);
    auto compType = sourceGroupConfig->GetParameter().GetCompressTypeOption();
    VarLenDataParam param;
    param.enableAdaptiveOffset = true;
    param.equalCompressOffset = compType.HasEquivalentCompress();
    param.dataItemUniqEncode = compType.HasUniqEncodeCompress();
    param.appendDataItemLength = compType.HasUniqEncodeCompress();
    param.disableGuardOffset = false;

    const auto& fileCompressConfig = sourceGroupConfig->GetParameter().GetFileCompressConfig();
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
} // namespace indexlibv2::index
