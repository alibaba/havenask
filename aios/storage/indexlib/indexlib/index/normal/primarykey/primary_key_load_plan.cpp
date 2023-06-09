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
#include "indexlib/index/normal/primarykey/primary_key_load_plan.h"

#include "indexlib/index/normal/primarykey/primary_key_loader.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PrimaryKeyLoadPlan);

PrimaryKeyLoadPlan::PrimaryKeyLoadPlan() : mBaseDocid(INVALID_DOCID), mDocCount(0) {}

PrimaryKeyLoadPlan::~PrimaryKeyLoadPlan() {}

void PrimaryKeyLoadPlan::Init(docid_t baseDocid, const PrimaryKeyIndexConfigPtr& indexConfig)
{
    mBaseDocid = baseDocid;
    mIndexConfig = indexConfig;
    mDocCount = 0;
}

bool PrimaryKeyLoadPlan::CanDirectLoad() const
{
    if (mSegmentDatas.size() > 1) {
        return false;
    }
    PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode =
        mIndexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode();
    PrimaryKeyIndexType pkIndexType = mIndexConfig->GetPrimaryKeyIndexType();
    if (loadMode == PrimaryKeyLoadStrategyParam::HASH_TABLE && pkIndexType == pk_hash_table) {
        return true;
    }

    if (loadMode == PrimaryKeyLoadStrategyParam::SORTED_VECTOR && pkIndexType == pk_sort_array) {
        return true;
    }

    if (loadMode == PrimaryKeyLoadStrategyParam::BLOCK_VECTOR && pkIndexType == pk_block_array) {
        return true;
    }
    assert(loadMode == PrimaryKeyLoadStrategyParam::HASH_TABLE);
    return false;
}

string PrimaryKeyLoadPlan::GetTargetFileName() const
{
    if (CanDirectLoad()) {
        return PRIMARY_KEY_DATA_FILE_NAME;
    }
    stringstream ss;
    // TODO
    ss << PRIMARY_KEY_DATA_SLICE_FILE_NAME;
    for (size_t i = 0; i < mSegmentDatas.size(); i++) {
        ss << "_" << mSegmentDatas[i].GetSegmentId();
    }
    return ss.str();
}

file_system::DirectoryPtr PrimaryKeyLoadPlan::GetTargetFileDirectory() const
{
    assert(mSegmentDatas.size() > 0);
    SegmentData maxSegData = mSegmentDatas[mSegmentDatas.size() - 1];
    return PrimaryKeyLoader<uint64_t>::GetPrimaryKeyDirectory(maxSegData.GetDirectory(), mIndexConfig);
}
}} // namespace indexlib::index
