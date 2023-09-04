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
#include "indexlib/index/attribute/MultiValueAttributeOffsetReader.h"

#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, MultiValueAttributeOffsetReader);

Status MultiValueAttributeOffsetReader::Init(const std::shared_ptr<AttributeConfig>& attrConfig,
                                             const std::shared_ptr<indexlib::file_system::IDirectory>& attrDir,
                                             uint32_t docCount, bool disableUpdate)
{
    _attrConfig = attrConfig;
    _varLenDataParam = VarLenDataParamHelper::MakeParamForAttribute(_attrConfig);
    Status status;
    if (_varLenDataParam.equalCompressOffset) {
        status = _compressOffsetReader.Init(attrConfig, attrDir, disableUpdate, docCount, _attributeMetrics);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "init attribute compress offset reader failed. ");
            return status;
        }
    } else {
        status = _unCompressOffsetReader.Init(attrConfig, attrDir, disableUpdate, docCount);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "init attribute un-compress offset reader failed. ");
            return status;
        }
    }
    return Status::OK();
}

const std::shared_ptr<indexlib::file_system::FileReader>& MultiValueAttributeOffsetReader::GetFileReader() const
{
    if (_varLenDataParam.equalCompressOffset) {
        return _compressOffsetReader.GetFileReader();
    }
    return _unCompressOffsetReader.GetFileReader();
}

} // namespace indexlibv2::index
