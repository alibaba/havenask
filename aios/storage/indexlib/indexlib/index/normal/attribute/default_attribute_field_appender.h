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
#ifndef __INDEXLIB_DEFAULT_ATTRIBUTE_FIELD_APPENDER_H
#define __INDEXLIB_DEFAULT_ATTRIBUTE_FIELD_APPENDER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(common, AttributeValueInitializer);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegmentReader);

namespace indexlib { namespace index {

// DefaultAttributeFieldAppender负责当add
// doc对应的原始doc中某些正排字段为空，但是正排对应schema要求正排有默认值占位的情况，此时DefaultAttributeFieldAppender会额外对doc做前置处理，加上正排的默认值。
class DefaultAttributeFieldAppender
{
public:
    typedef std::vector<common::AttributeValueInitializerPtr> AttrInitializerVector;

public:
    DefaultAttributeFieldAppender();
    ~DefaultAttributeFieldAppender();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              const index_base::InMemorySegmentReaderPtr& inMemSegmentReader);

    void AppendDefaultFieldValues(const document::NormalDocumentPtr& document);

private:
    void InitAttributeValueInitializers(const config::AttributeSchemaPtr& attrSchema,
                                        const index_base::InMemorySegmentReaderPtr& inMemSegmentReader);

    void InitEmptyFields(const document::NormalDocumentPtr& document, const config::AttributeSchemaPtr& attrSchema,
                         bool isVirtual);

private:
    config::IndexPartitionSchemaPtr mSchema;
    AttrInitializerVector mAttrInitializers;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultAttributeFieldAppender);
}} // namespace indexlib::index

#endif //__INDEXLIB_DEFAULT_ATTRIBUTE_FIELD_APPENDER_H
