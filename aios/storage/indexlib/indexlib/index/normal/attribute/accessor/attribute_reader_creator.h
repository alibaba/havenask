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

#ifndef __INDEXLIB_ATTRIBUTE_READER_CREATOR_H
#define __INDEXLIB_ATTRIBUTE_READER_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

#define DECLARE_ATTRIBUTE_READER_CREATOR(classname, indextype)                                                         \
    class classname##Creator : public AttributeReaderCreator                                                           \
    {                                                                                                                  \
    public:                                                                                                            \
        FieldType GetAttributeType() const { return indextype; }                                                       \
        AttributeReader* Create(AttributeMetrics* metrics = NULL) const { return new classname(metrics); }             \
    };

class AttributeReaderCreator
{
public:
    AttributeReaderCreator() {}
    virtual ~AttributeReaderCreator() {}

public:
    virtual FieldType GetAttributeType() const = 0;
    virtual AttributeReader* Create(AttributeMetrics* metrics = NULL) const = 0;
};

typedef std::shared_ptr<AttributeReaderCreator> AttributeReaderCreatorPtr;
}} // namespace indexlib::index

#endif //__INDEXLIB_ATTRIBUTE_READER_CREATOR_H
