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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, IndexSegmentReader);

namespace indexlib { namespace index {

class MultiFieldIndexSegmentReader
{
public:
    typedef std::map<std::string, std::shared_ptr<IndexSegmentReader>> String2IndexReaderMap;

public:
    MultiFieldIndexSegmentReader() {}
    ~MultiFieldIndexSegmentReader() {}

public:
    void AddIndexSegmentReader(const std::string& indexName,
                               const std::shared_ptr<IndexSegmentReader>& indexSegmentReader)
    {
        mIndexReaders.insert(make_pair(indexName, indexSegmentReader));
    }

    std::shared_ptr<IndexSegmentReader> GetIndexSegmentReader(const std::string& indexName) const
    {
        String2IndexReaderMap::const_iterator it = mIndexReaders.find(indexName);
        if (it != mIndexReaders.end()) {
            return it->second;
        }
        return std::shared_ptr<IndexSegmentReader>();
    }

private:
    String2IndexReaderMap mIndexReaders;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiFieldIndexSegmentReader);
}} // namespace indexlib::index
