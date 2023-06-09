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
#ifndef __INDEXLIB_REFERENCE_H
#define __INDEXLIB_REFERENCE_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/doc_info.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class Reference
{
public:
    Reference(size_t offset, FieldType fieldType, bool supportNull);
    virtual ~Reference();

public:
    size_t GetOffset() const;
    FieldType GetFieldType() const;

    virtual std::string GetStringValue(DocInfo* docInfo) = 0;

protected:
    size_t mOffset;
    bool mSupportNull;
    FieldType mFieldType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Reference);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_REFERENCE_H
