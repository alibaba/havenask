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
#include "indexlib/index/attribute/AttributeDiskIndexer.h"

#include "autil/EnvUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/patch/AttributePatchReader.h"
#include "indexlib/index/attribute/patch/DefaultValueAttributePatch.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeDiskIndexer);

void AttributeDiskIndexer::EnableGlobalReadContext()
{
    if (_globalCtx) {
        return;
    }

    _globalCtxSwitchLimit = autil::EnvUtil::getEnv("GLOBAL_READ_CONTEXT_SWITCH_MEMORY_LIMIT", 2 * 1024 * 1024);
    _globalCtxPool.reset(new autil::mem_pool::UnsafePool());
    _globalCtx = CreateReadContextPtr(_globalCtxPool.get());
}
size_t AttributeDiskIndexer::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                             const std::shared_ptr<indexlib::file_system::IDirectory>& attrDirectory)
{
    auto attrConfig = std::dynamic_pointer_cast<AttributeConfig>(indexConfig);
    assert(attrConfig != nullptr);
    if (!attrDirectory) {
        AUTIL_LOG(ERROR, "open without directory information.");
        return 0;
    }
    auto attrPath = GetAttributePath(attrConfig);
    auto fieldDir = indexlib::file_system::IDirectory::ToLegacyDirectory(attrDirectory)
                        ->GetDirectory(attrPath, /*throwExceptionIfNotExist=*/false);
    if (fieldDir == nullptr) {
        if (_indexerParam.readerOpenType == index::IndexerParameter::READER_DEFAULT_VALUE) {
            return 0;
        }
        if (_indexerParam.docCount != 0) {
            AUTIL_LOG(ERROR, "get attribute field dir [%s] failed", attrConfig->GetAttrName().c_str());
        }
        return 0;
    }
    return fieldDir->EstimateFileMemoryUse(ATTRIBUTE_DATA_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG) +
           fieldDir->EstimateFileMemoryUse(ATTRIBUTE_OFFSET_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG);
}

std::string AttributeDiskIndexer::GetAttributePath(const std::shared_ptr<AttributeConfig>& attrConfig)
{
    assert(attrConfig);
    auto indexPaths = attrConfig->GetIndexPath();
    assert(indexPaths.size() == 1);
    std::string prefixPath = attrConfig->GetIndexCommonPath();
    std::string attrPath;
    indexlib::util::PathUtil::GetRelativePath(prefixPath, indexPaths[0], attrPath);
    return attrPath;
}

Status AttributeDiskIndexer::SetPatchReader(const std::shared_ptr<AttributePatchReader>& patchReader,
                                            docid_t patchBaseDocId)
{
    if (_patch) {
        auto defaultValuePatch = std::dynamic_pointer_cast<DefaultValueAttributePatch>(_patch);
        if (defaultValuePatch) {
            defaultValuePatch->SetPatchReader(patchReader);
        } else {
            _patch = patchReader;
        }
    } else {
        _patch = patchReader;
    }
    if (_patch && _patch->GetMaxPatchItemLen() > _dataInfo.maxItemLen) {
        _dataInfo.maxItemLen = _patch->GetMaxPatchItemLen();
    }
    _patchBaseDocId = patchBaseDocId;
    return Status::OK();
}

} // namespace indexlibv2::index
