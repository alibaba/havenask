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
#include "autil/NoCopyable.h"
#include "autil/StringView.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::index {

class IAttributePatch : private autil::NoCopyable
{
public:
    IAttributePatch() = default;
    virtual ~IAttributePatch() {}

public:
    virtual std::pair<Status, size_t> Seek(docid_t docId, uint8_t* value, size_t maxLen, bool& isNull) = 0;
    virtual bool UpdateField(docid_t docId, const autil::StringView& value, bool isNull) = 0;
    virtual uint32_t GetMaxPatchItemLen() const = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
