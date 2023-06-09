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
#include "build_service/reader/IdleDocumentParser.h"

#include "build_service/document/DocumentDefine.h"

using namespace std;
using namespace build_service::document;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, IdleDocumentParser);

IdleDocumentParser::IdleDocumentParser(const string& fieldName) : _fieldName(fieldName) {}

IdleDocumentParser::~IdleDocumentParser() {}

bool IdleDocumentParser::parse(const string& docString, indexlib::document::RawDocument& rawDoc)
{
    rawDoc.setField(_fieldName, docString);
    rawDoc.setField(CMD_TAG, CMD_ADD);
    return true;
}

}} // namespace build_service::reader
