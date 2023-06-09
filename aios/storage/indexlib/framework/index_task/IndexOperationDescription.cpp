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
#include "indexlib/framework/index_task/IndexOperationDescription.h"

#include "autil/EnvUtil.h"

namespace indexlibv2::framework {

IndexOperationDescription::IndexOperationDescription(IndexOperationId id, const IndexOperationType& type)
    : _id(id)
    , _type(type)
{
    // legacy code, ut for compatible with old merge task
    if (!autil::EnvUtil::getEnv("use_op_fence_dir", true)) {
        return;
    }
    AddParameter("use_op_fence_dir", true);
}

IndexOperationDescription::IndexOperationDescription() : _id(-1)
{
    // legacy code, for compatible with old merge task
    if (!autil::EnvUtil::getEnv("use_op_fence_dir", true)) {
        return;
    }
    AddParameter("use_op_fence_dir", true);
}

void IndexOperationDescription::Jsonize(JsonWrapper& json)
{
    json.Jsonize("id", _id);
    json.Jsonize("type", _type);
    json.Jsonize("parameters", _parameters);
    json.Jsonize("depends", _depends);
    json.Jsonize("estimate_memory_use", _estimateMemUse);
}

bool IndexOperationDescription::UseOpFenceDir() const
{
    bool useOpFenceDir = false;
    bool exist = GetParameter("use_op_fence_dir", useOpFenceDir);
    return exist && useOpFenceDir;
}
bool IndexOperationDescription::IsDependOn(IndexOperationId id) const
{
    return std::find(_depends.begin(), _depends.end(), id) != _depends.end();
}

void IndexOperationDescription::AddDepends(const std::vector<IndexOperationId>& ids)
{
    _depends.insert(_depends.end(), ids.begin(), ids.end());
}

void IndexOperationDescription::AddDepend(IndexOperationId id)
{
    assert(!IsDependOn(id));
    _depends.push_back(id);
}

std::string IndexOperationDescription::GetOpFenceDirName() const { return GetOpFenceDirName(_id); }

std::string IndexOperationDescription::GetOpFenceDirName(IndexOperationId id)
{
    return autil::StringUtil::toString(id);
}

bool IndexOperationDescription::operator==(const IndexOperationDescription& other) const
{
    return _id == other._id && _type == other._type && _parameters == other._parameters && _depends == other._depends;
}

} // namespace indexlibv2::framework
