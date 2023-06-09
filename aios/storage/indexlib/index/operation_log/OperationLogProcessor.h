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
#include "autil/StringView.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlib::document {
class ModifiedTokens;
}
namespace indexlib::index {

class OperationLogProcessor
{
public:
    OperationLogProcessor() {}
    virtual ~OperationLogProcessor() {}

    virtual Status RemoveDocument(docid_t docid) = 0;
    virtual bool UpdateFieldValue(docid_t docId, const std::string& fieldName, const autil::StringView& value,
                                  bool isNull) = 0;
    virtual Status UpdateFieldTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens) = 0;
};

} // namespace indexlib::index
