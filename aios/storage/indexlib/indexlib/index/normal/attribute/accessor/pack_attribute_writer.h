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

#include "autil/ConstString.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_writer.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/MemBuffer.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib { namespace index {

class PackAttributeWriter : public StringAttributeWriter
{
public:
    typedef common::PackAttributeFormatter::PackAttributeField PackAttributeField;
    typedef common::PackAttributeFormatter::PackAttributeFields PackAttributeFields;

public:
    PackAttributeWriter(const config::PackAttributeConfigPtr& packAttrConfig);

    ~PackAttributeWriter();

public:
    bool UpdateEncodeFields(docid_t docId, const PackAttributeFields& packAttrFields);

private:
    static const size_t PACK_ATTR_BUFF_INIT_SIZE = 256 * 1024; // 256KB

private:
    config::PackAttributeConfigPtr mPackAttrConfig;
    common::PackAttributeFormatterPtr mPackAttrFormatter;
    util::MemBuffer mBuffer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeWriter);
}} // namespace indexlib::index
