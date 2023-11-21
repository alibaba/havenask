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

class BufferedAiThetaSegment : public aitheta2::IndexContainer::Segment
{
public:
    //! Index Container Pointer
    using Pointer = std::shared_ptr<BufferedAiThetaSegment>;

    //! Constructor
    BufferedAiThetaSegment(const indexlibv2::index::ann::IndexDataReaderPtr& reader,
                           const aitheta2::IndexUnpacker::SegmentMeta& meta)
        : _meta(meta)
        , _regionSize(meta.data_size() + meta.padding_size())
        , _reader(reader)
    {
    }

    //! Destructor
    ~BufferedAiThetaSegment(void) = default;

    //! Retrieve size of data
    size_t data_size(void) const override { return _meta.data_size(); }

    //! Retrieve crc of data
    uint32_t data_crc(void) const override { return _meta.data_crc(); }

    //! Retrieve size of padding
    size_t padding_size(void) const override { return _meta.padding_size(); }

    //! Read data from segment
    size_t read(size_t offset, const void** data, size_t len) override
    {
        if (offset + len > _regionSize) {
            if (offset > _regionSize) {
                offset = _regionSize;
            }
            len = _regionSize - offset;
        }
        _buffer.resize(len);
        *data = _buffer.data();
        size_t indexOffset = offset + _meta.data_offset();
        return _reader->Read((void*)_buffer.data(), len, indexOffset);
    }

    size_t fetch(size_t offset, void* buf, size_t len) const override
    {
        if (offset + len > _regionSize) {
            if (offset > _regionSize) {
                offset = _regionSize;
            }
            len = _regionSize - offset;
        }
        size_t indexOffset = offset + _meta.data_offset();
        return _reader->Read(buf, len, indexOffset);
    }

    //! Read data from segment
    bool read(aitheta2::IndexContainer::SegmentData* iovec, size_t count) override
    {
        size_t total = 0u;
        for (auto *it = iovec, *end = iovec + count; it != end; ++it) {
            ailego_false_if_false(it->offset + it->length <= _regionSize);
            total += it->length;
        }
        ailego_false_if_false(total != 0);

        _buffer.resize(total);
        char* buf = _buffer.data();
        for (auto *it = iovec, *end = iovec + count; it != end; ++it) {
            size_t indexOffset = it->offset + _meta.data_offset();
            ailego_false_if_false(_reader->Read(buf, it->length, indexOffset) == it->length);
            it->data = buf;
            buf += it->length;
        }
        return true;
    }

    //! Clone the segment
    aitheta2::IndexContainer::Segment::Pointer clone(void) override
    {
        return std::make_shared<BufferedAiThetaSegment>(*this);
    }

private:
    aitheta2::IndexUnpacker::SegmentMeta _meta;
    size_t _regionSize {0};
    indexlibv2::index::ann::IndexDataReaderPtr _reader;
    std::string _buffer;
};

} // namespace aitheta2
