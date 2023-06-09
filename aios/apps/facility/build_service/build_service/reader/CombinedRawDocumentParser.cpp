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
#include "build_service/reader/CombinedRawDocumentParser.h"

using namespace std;

using namespace indexlib::document;

namespace build_service { namespace reader {
BS_LOG_SETUP(build_service, CombinedRawDocumentParser);

bool CombinedRawDocumentParser::parse(const string& docString, RawDocument& rawDoc)
{
    size_t curParser = _lastParser;
    size_t retryCount = 0;
    do {
        if (_parsers[curParser]->parse(docString, rawDoc)) {
            _lastParser = curParser;
            return true;
        } else {
            rawDoc.clear();
            curParser = (curParser + 1) % _parserCount;
            retryCount++;
        }
    } while (retryCount < _parserCount);
    return false;
}

}} // namespace build_service::reader
