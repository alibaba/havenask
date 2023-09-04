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
#include "indexlib/index/ann/aitheta2/util/CustomizedAithetaContainer.h"

using namespace std;
namespace aitheta2 {

int CustomizedAiThetaContainer::load()
{
    auto read_data = [this](size_t offset, const void** data, size_t len) {
        return this->data_reader_->Read(const_cast<void**>(data), len, offset);
    };

    IndexUnpacker unpacker;
    if (!unpacker.unpack(read_data, data_reader_->GetLength(), checksum_validation_)) {
        LOG_ERROR("Failed to unpack file");
        return IndexError_UnpackIndex;
    }
    segments_ = std::move(*unpacker.mutable_segments());
    magic_ = unpacker.magic();
    return 0;
}

IndexContainer::Segment::Pointer CustomizedAiThetaContainer::get(const string& id) const
{
    if (!data_reader_) {
        return IndexContainer::Segment::Pointer();
    }

    auto it = segments_.find(id);
    if (it == segments_.end()) {
        return IndexContainer::Segment::Pointer();
    }
    return std::make_shared<Segment>(data_reader_, it->second);
}

//! Retrieve all segments
map<string, IndexContainer::Segment::Pointer> CustomizedAiThetaContainer::get_all(void) const
{
    if (!data_reader_) {
        return {};
    }

    map<string, IndexContainer::Segment::Pointer> result;
    for (const auto& it : segments_) {
        result.emplace(it.first, make_shared<Segment>(data_reader_, it.second));
    }
    return result;
}

aitheta2::IndexContainer::Segment::Pointer CustomizedAiThetaContainer::fetch(const std::string& id, int level) const
{
    auto segment = get(id);
    if (level > 0 || segment == nullptr) {
        return segment;
    }

    size_t region_size = segment->data_size() + segment->padding_size();
    assert(region_size > 0);
    shared_ptr<uint8_t> data(new (std::nothrow) uint8_t[region_size], [](uint8_t* p) { delete[] p; });
    if (data == nullptr) {
        LOG_ERROR("malloc [%lu]bytes failed", region_size);
        return aitheta2::IndexContainer::Segment::Pointer();
    }

    size_t size = segment->fetch(/*offset*/ 0ul, data.get(), region_size);
    if (size != region_size) {
        LOG_ERROR("fetch data failed, expected[%lu] vs actual size[%lu]", region_size, size);
        return aitheta2::IndexContainer::Segment::Pointer();
    }

    return Segment::Pointer(new Segment(data, segment->data_size(), segment->padding_size(), segment->data_crc()));
}

INDEX_FACTORY_REGISTER_CONTAINER(CustomizedAiThetaContainer);
} // namespace aitheta2