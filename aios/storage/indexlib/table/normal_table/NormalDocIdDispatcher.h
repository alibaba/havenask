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
#include "indexlib/base/Types.h"

namespace indexlibv2::document {
class NormalDocument;
}
namespace indexlib::index {
class PrimaryKeyIndexReader;
}

namespace indexlib::table {

class NormalDocIdDispatcher : private autil::NoCopyable
{
public:
    NormalDocIdDispatcher(const std::string& tableName, const std::shared_ptr<index::PrimaryKeyIndexReader>& pkReader,
                          docid_t buildingSegmentBaseDocId, docid_t currentSegmentDocCount);
    virtual ~NormalDocIdDispatcher() {};
    void DispatchDocId(const std::shared_ptr<indexlibv2::document::NormalDocument>& doc);

protected:
    void ProcessAddDocument(const std::shared_ptr<indexlibv2::document::NormalDocument>& doc);
    void ProcessUpdateDocument(const std::shared_ptr<indexlibv2::document::NormalDocument>& doc);
    void ProcessDeleteDocument(const std::shared_ptr<indexlibv2::document::NormalDocument>& doc);

private:
    docid_t Lookup(const std::string& pkStr) const;
    void Update(const std::string& pk, docid_t docId);
    void Remove(const std::string& pk);

private:
    std::string _tableName;
    std::shared_ptr<index::PrimaryKeyIndexReader> _pkReader;
    docid_t _buildingSegmentBaseDocId;
    docid_t _currentSegmentDocCount;
    std::map<std::string, docid_t> _pkToDocIdMap;
    std::map<docid_t, std::string> _docIdToPkMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::table
