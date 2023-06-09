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
#include "indexlib/partition/remote_access/attribute_data_patcher.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::common;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, AttributeDataPatcher);

AttributeDataPatcher::AttributeDataPatcher()
    : mTotalDocCount(0)
    , mPatchedDocCount(0)
    , mPool(POOL_CHUNK_SIZE)
    , mSupportNull(false)
{
}

AttributeDataPatcher::~AttributeDataPatcher()
{
    assert(!mPatchDataWriter);
    if (mPatchDataWriter) {
        IE_LOG(ERROR, "patcher for [%s] is not closed!", mAttrConf->GetAttrName().c_str());
    }
}

bool AttributeDataPatcher::Init(const AttributeConfigPtr& attrConfig, const config::MergeIOConfig& mergeIOConfig,
                                const file_system::DirectoryPtr& segDir, uint32_t docCount)
{
    mAttrConf = attrConfig;
    if (mAttrConf && mAttrConf->SupportNull()) {
        mSupportNull = true;
        mNullString = mAttrConf->GetFieldConfig()->GetNullFieldLiteralString();
    }

    mTotalDocCount = docCount;
    mPatchedDocCount = 0;
    mAttrConvertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(mAttrConf));
    if (!mAttrConvertor) {
        IE_LOG(ERROR, "create attribute convertor fail!");
        return false;
    }
    mAttrConvertor->SetEncodeEmpty(true);

    if (mTotalDocCount == 0) {
        IE_LOG(INFO, "Init AttributeDataPatcher [%s] for empty segment [%s]", attrConfig->GetAttrName().c_str(),
               segDir->DebugString().c_str());
        return true;
    }
    auto attrDir = segDir->MakeDirectory(ATTRIBUTE_DIR_NAME);

    if (attrDir->IsExist(attrConfig->GetAttrName())) {
        IE_LOG(ERROR, "target path [%s] already exist, will be cleaned!", attrConfig->GetAttrName().c_str());
        file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
        attrDir->RemoveDirectory(attrConfig->GetAttrName(), removeOption /*mayNonExist*/);
    }
    mDirectory = attrDir->MakeDirectory(attrConfig->GetAttrName());
    return DoInit(mergeIOConfig);
}

void AttributeDataPatcher::AppendFieldValue(const string& valueStr)
{
    StringView inputStr = StringView(valueStr);
    AppendFieldValue(inputStr);
}

void AttributeDataPatcher::AppendAllDocByDefaultValue()
{
    assert(mPatchedDocCount == 0);
    IE_LOG(INFO, "append all doc by default value");
    if (mAttrConf->SupportNull()) {
        for (uint32_t i = mPatchedDocCount; i < mTotalDocCount; i++) {
            AppendNullValue();
        }
        return;
    }

    const string& defaultValue = mAttrConf->GetFieldConfig()->GetDefaultValue();
    StringView inputStr = StringView(defaultValue);
    assert(mAttrConvertor);
    StringView encodeValue = mAttrConvertor->Encode(inputStr, &mPool);
    for (uint32_t i = mPatchedDocCount; i < mTotalDocCount; i++) {
        AppendEncodedValue(encodeValue);
    }
}

void AttributeDataPatcher::AppendNullValue()
{
    if (mPatchedDocCount >= mTotalDocCount) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "patchDocCount [%u] over totalDocCount [%u]", mPatchedDocCount,
                             mTotalDocCount);
    }
    assert(mPatchDataWriter);
    mPatchDataWriter->AppendNullValue();
    mPatchedDocCount++;
}
}} // namespace indexlib::partition
