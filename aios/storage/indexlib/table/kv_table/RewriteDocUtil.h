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
#include "autil/Log.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
namespace indexlibv2::base {
class PartitionResponse;
}
namespace indexlibv2::table {
class RewriteDocUtil
{
public:
    static Status RewriteMergeValue(const autil::StringView& currentValue, const autil::StringView& newValue,
                                    std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& formatter,
                                    indexlib::util::MemBufferPtr& memBuffer, document::KVDocument* doc);
    static void RewriteValue(std::shared_ptr<indexlibv2::index::AttributeConvertor>& attrConvertor,
                             autil::StringView& value);

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlibv2::table