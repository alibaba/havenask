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
#include "indexlib_plugin/plugins/aitheta/index/offline_index_attr.h"
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;
using namespace aitheta;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::file_system;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, OfflineIndexAttrHolder);

void OfflineIndexAttr::Jsonize(JsonWrapper &json) {
    json.Jsonize("catid", catid, -1L);
    json.Jsonize("num", docNum, 0ul);
    json.Jsonize("offset", offset, 0ul);
    json.Jsonize("size", size, 0ul);

    if (json.GetMode() == Jsonizable::Mode::TO_JSON) {
        uint32_t m = reformer.m();
        float u = reformer.u();
        float norm = reformer.norm();
        json.Jsonize("mips_m", m, 0u);
        json.Jsonize("mips_u", u, 0.0f);
        json.Jsonize("mips_norm", norm, 0.0f);
    } else {
        uint32_t m = 0u;
        float u = 0.0f;
        float norm = 0.0f;
        json.Jsonize("mips_m", m, 0u);
        json.Jsonize("mips_u", u, 0.0f);
        json.Jsonize("mips_norm", norm, 0.0f);
        reformer = MipsReformer(m, u, norm);
    }
}

bool OfflineIndexAttrHolder::Load(const indexlib::file_system::DirectoryPtr &dir) {
    auto reader = IndexlibIoWrapper::CreateReader(dir, INDEX_ATTR_FILE, FSOT_MMAP);
    if (!reader) {
        return false;
    }
    string str((char *)reader->GetBaseAddress(), reader->GetLength());
    reader->Close().GetOrThrow();
    try {
        FromJsonString(mOfflineIndexAttrs, str);
    } catch (const ExceptionBase &e) {
        IE_LOG(ERROR, "failed to jsonize str[%s] in path[%s], error[%s]", str.c_str(), dir->DebugString().c_str(),
               e.what());
        return false;
    }
    return true;
}

bool OfflineIndexAttrHolder::Dump(const indexlib::file_system::DirectoryPtr &dir) {
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    dir->RemoveFile(INDEX_ATTR_FILE, removeOption);
    auto writer = IndexlibIoWrapper::CreateWriter(dir, INDEX_ATTR_FILE);
    if (!writer) {
        return false;
    }

    string str;
    try {
        str = ToJsonString(mOfflineIndexAttrs);
    } catch (const ExceptionBase &e) {
        writer->Close().GetOrThrow();
        IE_LOG(ERROR, "failed to jsonize str in path[%s], error[%s]", writer->DebugString().c_str(), e.what());
        return false;
    }
    writer->Write(str.data(), str.size()).GetOrThrow();
    writer->Close().GetOrThrow();
    return true;
}

}
}
