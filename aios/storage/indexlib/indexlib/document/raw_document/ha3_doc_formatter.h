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
#include "indexlib/document/raw_document.h"

namespace indexlib::document {

class Ha3DocFormatter : private autil::NoCopyable
{
public:
    Ha3DocFormatter() {}
    ~Ha3DocFormatter() {}

public:
    static bool ToString(RawDocument& rawDoc, std::string& str)
    {
        std::unique_ptr<RawDocFieldIterator> iter(rawDoc.CreateIterator());
        if (!iter) {
            return false;
        }

        str.clear();
        str.reserve(1024);
        str += "CMD=";
        switch (rawDoc.getDocOperateType()) {
        case ADD_DOC: {
            str += "add";
            break;
        }
        case DELETE_DOC: {
            str += "delete";
            break;
        }
        case UPDATE_FIELD: {
            str += "update_field";
            break;
        }
        default:
            return false;
        }

        str += "\n";
        while (iter->IsValid()) {
            autil::StringView fieldName = iter->GetFieldName();
            autil::StringView fieldValue = iter->GetFieldValue();
            str += std::string(fieldName.data(), fieldName.size());
            str += "=";
            str += std::string(fieldValue.data(), fieldValue.size());
            str += "\n";
            iter->MoveToNext();
        }
        str += "\n";
        return true;
    }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::document
