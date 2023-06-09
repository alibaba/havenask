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
#include "indexlib/index/kkv/kkv_reader_impl_factory.h"

#include "indexlib/index/kkv/kkv_define.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {

KKVReaderImplFactory::KKVReaderImplFactory() {}

KKVReaderImplFactory::~KKVReaderImplFactory() {}

KKVReaderImplBase* KKVReaderImplFactory::Create(const KKVIndexConfigPtr& kkvConfig, const std::string& schemaName,
                                                std::string& readerType)
{
    FieldType suffixKeyFieldType = GetSKeyDictKeyType(kkvConfig->GetSuffixFieldConfig()->GetFieldType());

#define KKVREADER_OPEN_MACRO(type)                                                                                     \
    case type: {                                                                                                       \
        typedef FieldTypeTraits<type>::AttrItemType SKeyType;                                                          \
        readerType = string("KKVReaderImpl<") + FieldTypeTraits<type>::GetTypeName() + ">";                            \
        return new KKVReaderImpl<SKeyType>(schemaName);                                                                \
    }

    switch (suffixKeyFieldType) {
        NUMBER_FIELD_MACRO_HELPER(KKVREADER_OPEN_MACRO);
    default: {
        assert(false);
    }
    }
#undef KKVREADER_OPEN_MACRO
    return NULL;
}
}} // namespace indexlib::index
