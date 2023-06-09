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
#ifndef __INDEXLIB_KKV_MERGER_CREATOR_H
#define __INDEXLIB_KKV_MERGER_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/kkv_merger_typed.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace index {

class KKVMergerCreator
{
public:
    KKVMergerCreator();
    ~KKVMergerCreator();

public:
    static KKVMergerPtr Create(int64_t currentTs, const config::IndexPartitionSchemaPtr& schema, bool keepTs)
    {
        assert(schema);
        KKVMergerPtr kkvMerger;
        config::KKVIndexConfigPtr kkvConfig = KKVMerger::GetDataKKVIndexConfig(schema);
        FieldType skeyDictType = GetSKeyDictKeyType(kkvConfig->GetSuffixFieldConfig()->GetFieldType());
        switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        typedef config::FieldTypeTraits<type>::AttrItemType SKeyType;                                                  \
        KKVMergerTyped<SKeyType>* merger = new KKVMergerTyped<SKeyType>(currentTs, schema, keepTs);                    \
        kkvMerger.reset(merger);                                                                                       \
        break;                                                                                                         \
    }
            NUMBER_FIELD_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        default: {
            assert(false);
            break;
        }
        }
        return kkvMerger;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVMergerCreator);
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_MERGER_CREATOR_H
