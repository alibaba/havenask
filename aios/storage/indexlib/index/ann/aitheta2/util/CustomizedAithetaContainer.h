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

#include "autil/Diagnostic.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wsuggest-override")
#include <aitheta2/index_error.h>
#include <aitheta2/index_factory.h>
#include <aitheta2/index_format.h>
#include <aitheta2/index_unpacker.h>
DIAGNOSTIC_POP
#include <cerrno>

#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataReader.h"

namespace aitheta2 {

class CustomizedAiThetaContainer : public aitheta2::IndexContainer
{
public:
    CustomizedAiThetaContainer(const indexlibv2::index::ann::IndexDataReaderPtr& reader) : _reader(reader) {}
    CustomizedAiThetaContainer() = default;
    ~CustomizedAiThetaContainer() = default;

public:
    // Initialize container
    int init(const aitheta2::IndexParams& params) override { return 0; }
    // Cleanup container
    int cleanup(void) override { return this->unload(); }
    // Load a index file into container
    int load() override;
    // Load with path
    int load(const std::string& path) override { return load(); }
    // Unload all indexes
    int unload(void) override
    {
        _reader.reset();
        _segments.clear();
        return 0;
    }
    bool has(const std::string& id) const override { return (_segments.find(id) != _segments.end()); }
    //! Retrieve a segment by id
    aitheta2::IndexContainer::Segment::Pointer get(const std::string& id) const override;
    //! Retrieve all segments
    std::map<std::string, aitheta2::IndexContainer::Segment::Pointer> get_all(void) const override;
    //! Retrieve magic number of index
    uint32_t magic(void) const override { return _magic; }
    //! Fetch a segment by id with level (0 high, 1 normal, 2 low)
    aitheta2::IndexContainer::Segment::Pointer fetch(const std::string& id,
                                                     aitheta2::IndexContainer::FetchLevel level) const override;

private:
    bool _checksumValidation {false};
    uint32_t _magic {0};
    std::map<std::string, aitheta2::IndexUnpacker::SegmentMeta> _segments {};
    indexlibv2::index::ann::IndexDataReaderPtr _reader;
    std::string buffer_;
};

} // namespace aitheta2
