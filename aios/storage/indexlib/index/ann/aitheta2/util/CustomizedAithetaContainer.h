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
    class Segment : public aitheta2::IndexContainer::Segment
    {
    public:
        //! Index Container Pointer
        typedef std::shared_ptr<Segment> Pointer;

        //! Constructor
        Segment(const indexlibv2::index::ann::IndexDataReaderPtr& data_reader,
                const aitheta2::IndexUnpacker::SegmentMeta& meta)
            : data_(reinterpret_cast<uint8_t*>(data_reader->GetIndexFileAddress()) + meta.data_offset())
            , data_size_(meta.data_size())
            , padding_size_(meta.padding_size())
            , region_size_(meta.data_size() + meta.padding_size())
            , data_crc_(meta.data_crc())
            , data_reader_(data_reader)
        {
        }

        //! Constructor
        Segment(const std::shared_ptr<uint8_t>& data_holder, size_t data_size, size_t padding_size, uint32_t data_crc)
            : data_(data_holder.get())
            , data_size_(data_size)
            , padding_size_(padding_size)
            , region_size_(data_size + padding_size)
            , data_crc_(data_crc)
            , data_holder_(data_holder)
        {
        }

        Segment(const Segment& rhs) = default;

        //! Destructor
        ~Segment(void) {}

        //! Retrieve size of data
        size_t data_size(void) const override { return data_size_; }

        //! Retrieve crc of data
        uint32_t data_crc(void) const override { return data_crc_; }

        //! Retrieve size of padding
        size_t padding_size(void) const override { return padding_size_; }

        //! Read data from segment
        size_t read(size_t offset, const void** data, size_t len) override
        {
            if (ailego_unlikely(offset + len > region_size_)) {
                if (offset > region_size_) {
                    offset = region_size_;
                }
                len = region_size_ - offset;
            }
            *data = data_ + offset;
            return len;
        }

        size_t fetch(size_t offset, void* buf, size_t len) const override
        {
            if (ailego_unlikely(offset + len > region_size_)) {
                if (offset > region_size_) {
                    offset = region_size_;
                }
                len = region_size_ - offset;
            }
            const void* data = data_ + offset;
            memcpy(buf, data, len);
            return len;
        }

        //! Read data from segment
        bool read(SegmentData* iovec, size_t count) override
        {
            for (auto* end = iovec + count; iovec != end; ++iovec) {
                ailego_false_if_false(iovec->offset + iovec->length <= region_size_);
                iovec->data = data_ + iovec->offset;
            }
            return true;
        }

        //! Clone the segment
        aitheta2::IndexContainer::Segment::Pointer clone(void) override { return std::make_shared<Segment>(*this); }

    private:
        const uint8_t* const data_ {nullptr};
        size_t data_size_ {0u};
        size_t padding_size_ {0u};
        size_t region_size_ {0u};
        uint32_t data_crc_ {0u};
        // not set data_holder_ and data_reader_ at the same time
        indexlibv2::index::ann::IndexDataReaderPtr data_reader_ {};
        std::shared_ptr<uint8_t> data_holder_;
    };

public:
    CustomizedAiThetaContainer(const indexlibv2::index::ann::IndexDataReaderPtr& reader) : data_reader_(reader) {}
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
        data_reader_.reset();
        segments_.clear();
        return 0;
    }
    bool has(const std::string& id) const override { return (segments_.find(id) != segments_.end()); }
    //! Retrieve a segment by id
    aitheta2::IndexContainer::Segment::Pointer get(const std::string& id) const override;
    //! Retrieve all segments
    std::map<std::string, aitheta2::IndexContainer::Segment::Pointer> get_all(void) const override;
    //! Retrieve magic number of index
    uint32_t magic(void) const override { return magic_; }
    //! Fetch a segment by id with level (0 high, 1 normal, 2 low)
    aitheta2::IndexContainer::Segment::Pointer fetch(const std::string& id, int /*level*/) const override;

private:
    bool checksum_validation_ {false};
    uint32_t magic_ {0};
    std::map<std::string, aitheta2::IndexUnpacker::SegmentMeta> segments_ {};
    indexlibv2::index::ann::IndexDataReaderPtr data_reader_;
};

} // namespace aitheta2
