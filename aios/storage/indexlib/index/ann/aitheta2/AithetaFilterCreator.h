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

#include <functional>

#include "indexlib/index/ann/Common.h"
#include "indexlib/index/ann/aitheta2/AithetaAuxSearchInfo.h"
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"

namespace indexlibv2::index::ann {

class AithetaFilterCreatorBase
{
public:
    typedef std::function<bool(docid_t)> AithetaFilterFunc;

public:
    AithetaFilterCreatorBase() = default;
    virtual ~AithetaFilterCreatorBase() = default;

public:
    virtual bool Create(docid_t segmentBaseDocId, std::shared_ptr<AithetaFilterBase> filter,
                        AithetaFilterFunc& func) = 0;
};

class AithetaFilterCreator : public AithetaFilterCreatorBase
{
public:
    AithetaFilterCreator(const std::shared_ptr<index::DeletionMapIndexReader>& reader) : _deletionMapReader(reader) {}
    ~AithetaFilterCreator() = default;

public:
    bool Create(docid_t segmentBaseDocId, std::shared_ptr<AithetaFilterBase> filter, AithetaFilterFunc& func) override
    {
        if (nullptr == _deletionMapReader && nullptr == filter) {
            return false;
        }
        if (nullptr != _deletionMapReader && nullptr != filter) {
            func = [reader = _deletionMapReader, segmentBaseDocId, filter](docid_t docId) {
                docid_t newDocId = segmentBaseDocId + docId;
                return reader->IsDeleted(newDocId) || (*filter)(newDocId);
            };
        } else if (nullptr != _deletionMapReader) {
            func = [reader = _deletionMapReader, segmentBaseDocId](docid_t docId) {
                return reader->IsDeleted(segmentBaseDocId + docId);
            };
        } else if (nullptr != filter) {
            func = [segmentBaseDocId, filter](docid_t docId) { return (*filter)(segmentBaseDocId + docId); };
        }
        return true;
    }

private:
    std::shared_ptr<index::DeletionMapIndexReader> _deletionMapReader;
};

} // namespace indexlibv2::index::ann