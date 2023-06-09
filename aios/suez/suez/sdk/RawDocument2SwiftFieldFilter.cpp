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
#include "RawDocument2SwiftFieldFilter.h"

#include "indexlib/document/raw_document.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "swift/common/FieldGroupWriter.h"

using namespace std;

namespace suez {

vector<string> convertRawDocument2SwiftFieldFilter(const std::vector<indexlib::document::RawDocumentPtr> &docs) {
    vector<string> results;
    swift::common::FieldGroupWriter writer;
    for (const auto &doc : docs) {
        auto iter = doc->CreateIterator();
        for (; iter->IsValid(); iter->MoveToNext()) {
            if (iter->GetFieldValue().data() == nullptr) {
                continue;
            }
            writer.addProductionField(iter->GetFieldName(), iter->GetFieldValue(), true);
        }
        delete iter;
        if (doc->exist(indexlib::document::CMD_TAG)) {
            writer.addProductionField(indexlib::document::CMD_TAG, doc->getField(indexlib::document::CMD_TAG));
        }
        results.push_back(writer.toString());
        writer.reset();
    }
    return results;
}

} // namespace suez
