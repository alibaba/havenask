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
#include "indexlib/index/kkv/common/KKVIndexFormat.h"

#include "indexlib/index/kkv/Constant.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, KKVIndexFormat);

KKVIndexFormat::KKVIndexFormat() : _storeTs(true), _keepSortSeq(false), _valueInline(false) {}

KKVIndexFormat::KKVIndexFormat(bool storeTs, bool keepSortSeq, bool valueInline)
    : _storeTs(storeTs)
    , _keepSortSeq(keepSortSeq)
    , _valueInline(valueInline)
{
}

KKVIndexFormat::~KKVIndexFormat() {}

Status KKVIndexFormat::Store(const std::shared_ptr<indexlib::file_system::IDirectory>& dir)
{
    std::string fileContent = ToString();
    if (dir->IsExist(KKV_INDEX_FORMAT_FILE).Value()) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(
            dir->RemoveFile(KKV_INDEX_FORMAT_FILE, indexlib::file_system::RemoveOption()).Status());
    }
    return dir->Store(KKV_INDEX_FORMAT_FILE, fileContent, indexlib::file_system::WriterOption()).Status();
}

Status KKVIndexFormat::Load(const std::shared_ptr<indexlib::file_system::IDirectory>& dir)
{
    std::string fileContent;
    RETURN_STATUS_DIRECTLY_IF_ERROR(
        dir->Load(KKV_INDEX_FORMAT_FILE,
                  indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM), fileContent)
            .Status());
    FromString(fileContent);
    return Status::OK();
}

std::string KKVIndexFormat::ToString() { return autil::legacy::ToJsonString(*this); }

void KKVIndexFormat::FromString(const std::string& content) { autil::legacy::FromJsonString(*this, content); }

void KKVIndexFormat::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("store_ts", _storeTs, _storeTs);
    json.Jsonize("keep_sort_sequence", _keepSortSeq, _keepSortSeq);
    json.Jsonize("value_inline", _valueInline, _valueInline);
}
} // namespace indexlibv2::index
