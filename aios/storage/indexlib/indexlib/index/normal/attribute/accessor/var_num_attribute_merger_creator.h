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
#pragma once

#include <memory>

#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_creator.h"
#include "indexlib/index/normal/attribute/accessor/uniq_encoded_var_num_attribute_merger.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class VarNumAttributeMergerCreator : public AttributeMergerCreator
{
public:
    FieldType GetAttributeType() const { return common::TypeInfo<T>::GetFieldType(); }

    AttributeMerger* Create(bool isUniqEncoded, bool isUpdatable, bool needMergePatch) const
    {
        if (isUniqEncoded) {
            return new UniqEncodedVarNumAttributeMerger<T>(needMergePatch);
        } else {
            return new VarNumAttributeMerger<T>(needMergePatch);
        }
    }
};
}} // namespace indexlib::index
