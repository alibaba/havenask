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
#ifndef __INDEXLIB_MODIFY_ITEM_H
#define __INDEXLIB_MODIFY_ITEM_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class ModifyItem : public autil::legacy::Jsonizable
{
public:
    ModifyItem();
    ModifyItem(const std::string& name, const std::string& type, schema_opid_t opId);
    ~ModifyItem();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const std::string& GetName() const { return mName; }
    const std::string& GetType() const { return mType; }
    schema_opid_t GetModifyOperationId() const { return mOpId; }

private:
    std::string mName;
    std::string mType;
    schema_opid_t mOpId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ModifyItem);
typedef std::vector<ModifyItemPtr> ModifyItemVector;
}} // namespace indexlib::config

#endif //__INDEXLIB_MODIFY_ITEM_H
