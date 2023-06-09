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
#include "indexlib/index/kv/KVFormatOptions.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/kv/KVCommonDefine.h"

namespace indexlibv2::index {

void KVFormatOptions::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("is_short_offset", _shortOffset);
    json.Jsonize("use_compact_bucket", _useCompactBucket, _useCompactBucket);
}

std::string KVFormatOptions::ToString() const { return ToJsonString(*this); }

void KVFormatOptions::FromString(const std::string& str) { FromJsonString(*this, str); }

Status KVFormatOptions::Load(const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    return Load(directory, *this);
}

Status KVFormatOptions::Store(const std::shared_ptr<indexlib::file_system::Directory>& directory) const
{
    return Store(directory, *this);
}

Status KVFormatOptions::Load(const std::shared_ptr<indexlib::file_system::Directory>& directory, KVFormatOptions& opts)
{
    std::string content;
    auto s = directory->GetIDirectory()
                 ->Load(KV_FORMAT_OPTION_FILE_NAME,
                        indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_MEM), content)
                 .Status();
    if (!s.IsOK()) {
        return s;
    }
    try {
        opts.FromString(content);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::Corruption(e.what());
    }
}

Status KVFormatOptions::Store(const std::shared_ptr<indexlib::file_system::Directory>& directory,
                              const KVFormatOptions& opts)
{
    return directory->GetIDirectory()
        ->Store(KV_FORMAT_OPTION_FILE_NAME, opts.ToString(), indexlib::file_system::WriterOption())
        .Status();
}

} // namespace indexlibv2::index
