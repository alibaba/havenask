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

#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataReader.h"

namespace aitheta2 {

class InMemoryAiThetaSegment : public aitheta2::IndexContainer::Segment
{
public:
    //! Index Container Pointer
    using Pointer = std::shared_ptr<Segment>;

    //! Constructor
    InMemoryAiThetaSegment(const indexlibv2::index::ann::IndexDataReaderPtr& reader,
                           const aitheta2::IndexUnpacker::SegmentMeta& meta)
        : _data(reader->GetBaseAddress() + meta.data_offset())
        , _dataSize(meta.data_size())
        , _paddingSize(meta.padding_size())
        , _regionSize(meta.data_size() + meta.padding_size())
        , _dataCrc(meta.data_crc())
        , _indexDataReader(reader)
    {
        assert(reader->GetBaseAddress() != nullptr);
    }

    //! Constructor
    InMemoryAiThetaSegment(const std::shared_ptr<char>& segmentData, size_t dataSize, size_t paddingSize,
                           uint32_t dataCrc)
        : _data(segmentData.get())
        , _dataSize(dataSize)
        , _paddingSize(paddingSize)
        , _regionSize(dataSize + paddingSize)
        , _dataCrc(dataCrc)
        , _segmentData(segmentData)
    {
        assert(segmentData != nullptr);
    }

    //! Destructor
    ~InMemoryAiThetaSegment(void) = default;

    //! Retrieve size of data
    size_t data_size(void) const override { return _dataSize; }

    //! Retrieve crc of data
    uint32_t data_crc(void) const override { return _dataCrc; }

    //! Retrieve size of padding
    size_t padding_size(void) const override { return _paddingSize; }

    //! Read data from segment
    size_t read(size_t offset, const void** data, size_t len) override
    {
        if (offset + len > _regionSize) {
            if (offset > _regionSize) {
                offset = _regionSize;
            }
            len = _regionSize - offset;
        }
        *data = _data + offset;
        return len;
    }

    size_t fetch(size_t offset, void* buf, size_t len) const override
    {
        if (offset + len > _regionSize) {
            if (offset > _regionSize) {
                offset = _regionSize;
            }
            len = _regionSize - offset;
        }
        const void* data = _data + offset;
        memcpy(buf, data, len);
        return len;
    }

    //! Read data from segment
    bool read(aitheta2::IndexContainer::SegmentData* iovec, size_t count) override
    {
        for (auto* end = iovec + count; iovec != end; ++iovec) {
            ailego_false_if_false(iovec->offset + iovec->length <= _regionSize);
            iovec->data = _data + iovec->offset;
        }
        return true;
    }

    //! Clone the segment
    aitheta2::IndexContainer::Segment::Pointer clone(void) override
    {
        return std::make_shared<InMemoryAiThetaSegment>(*this);
    }

private:
    const char* const _data {nullptr};
    size_t _dataSize {0u};
    size_t _paddingSize {0u};
    size_t _regionSize {0u};
    uint32_t _dataCrc {0u};
    // not set _segmentData and _indexDataReader at the same time
    indexlibv2::index::ann::IndexDataReaderPtr _indexDataReader {};
    std::shared_ptr<char> _segmentData;
};

} // namespace aitheta2
