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
#include "indexlib/index/kkv/kkv_index_format.h"

#include "indexlib/index_define.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVIndexFormat);

KKVIndexFormat::KKVIndexFormat() : mStoreTs(true), mKeepSortSeq(false), mValueInline(false) {}

KKVIndexFormat::KKVIndexFormat(bool storeTs, bool keepSortSeq, bool valueInline)
    : mStoreTs(storeTs)
    , mKeepSortSeq(keepSortSeq)
    , mValueInline(valueInline)
{
}

KKVIndexFormat::~KKVIndexFormat() {}

void KKVIndexFormat::Store(const file_system::DirectoryPtr& dir)
{
    string fileContent = ToString();
    if (dir->IsExist(KKV_INDEX_FORMAT_FILE)) {
        dir->RemoveFile(KKV_INDEX_FORMAT_FILE);
    }
    dir->Store(KKV_INDEX_FORMAT_FILE, fileContent, file_system::WriterOption());
}

void KKVIndexFormat::Load(const file_system::DirectoryPtr& dir)
{
    string fileContent;
    dir->Load(KKV_INDEX_FORMAT_FILE, fileContent, true);
    FromString(fileContent);
}

string KKVIndexFormat::ToString() { return autil::legacy::ToJsonString(*this); }

void KKVIndexFormat::FromString(const std::string& content) { autil::legacy::FromJsonString(*this, content); }

void KKVIndexFormat::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("store_ts", mStoreTs, mStoreTs);
    json.Jsonize("keep_sort_sequence", mKeepSortSeq, mKeepSortSeq);
    json.Jsonize("value_inline", mValueInline, mValueInline);
}
}} // namespace indexlib::index
