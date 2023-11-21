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
#include "indexlib/index/normal/source/source_define.h"

#include "autil/StringUtil.h"
#include "indexlib/index_define.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SourceDefine);

SourceDefine::SourceDefine() {}

SourceDefine::~SourceDefine() {}

std::string SourceDefine::GetDataDir(sourcegroupid_t groupId)
{
    return std::string(SOURCE_DATA_DIR_PREFIX) + "_" + autil::StringUtil::toString(groupId);
}
}} // namespace indexlib::index
