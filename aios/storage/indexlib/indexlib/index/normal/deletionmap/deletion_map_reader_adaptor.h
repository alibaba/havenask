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
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"

namespace indexlib::index {
// TODO(xiaohao.yxh) remove this class in the future
class DeletionMapReaderAdaptor
{
public:
    explicit DeletionMapReaderAdaptor(const std::shared_ptr<indexlib::index::DeletionMapReader>& reader)
        : _deletionMapReader(reader)
    {
    }

    explicit DeletionMapReaderAdaptor(const std::shared_ptr<indexlibv2::index::DeletionMapIndexReader>& reader)
        : _deletionMapReaderV2(reader)
    {
    }
    ~DeletionMapReaderAdaptor() {}

    bool IsDeleted(docid_t docid) const
    {
        if (_deletionMapReader) {
            return _deletionMapReader->IsDeleted(docid);
        }
        if (_deletionMapReaderV2) {
            return _deletionMapReaderV2->IsDeleted(docid);
        }
        return false;
    }

public:
    std::shared_ptr<indexlib::index::DeletionMapReader> _deletionMapReader;
    std::shared_ptr<indexlibv2::index::DeletionMapIndexReader> _deletionMapReaderV2;
};

} // namespace indexlib::index
