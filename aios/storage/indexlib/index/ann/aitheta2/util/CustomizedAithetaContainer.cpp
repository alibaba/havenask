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

#include "indexlib/index/ann/aitheta2/util/BufferedAithetaSegment.h"
#include "indexlib/index/ann/aitheta2/util/InMemoryAithetaSegment.h"

using namespace std;
namespace aitheta2 {

int CustomizedAiThetaContainer::load()
{
    auto readData = [this](size_t offset, const void** data, size_t len) {
        buffer_.resize(len);
        *data = buffer_.data();
        return _reader->Read((void*)buffer_.data(), len, offset);
    };

    IndexUnpacker unpacker;
    if (!unpacker.unpack(readData, _reader->GetLength(), _checksumValidation)) {
        LOG_ERROR("Failed to unpack file");
        return IndexError_UnpackIndex;
    }

    _segments = std::move(*unpacker.mutable_segments());
    _magic = unpacker.magic();
    return 0;
}

IndexContainer::Segment::Pointer CustomizedAiThetaContainer::get(const string& id) const
{
    if (_reader == nullptr) {
        return IndexContainer::Segment::Pointer();
    }

    auto it = _segments.find(id);
    if (it == _segments.end()) {
        return IndexContainer::Segment::Pointer();
    }
    if (_reader->GetBaseAddress()) {
        return std::make_shared<InMemoryAiThetaSegment>(_reader, it->second);
    } else {
        return std::make_shared<BufferedAiThetaSegment>(_reader, it->second);
    }
}

//! Retrieve all segments
map<string, IndexContainer::Segment::Pointer> CustomizedAiThetaContainer::get_all(void) const
{
    if (_reader == nullptr) {
        return {};
    }

    map<string, IndexContainer::Segment::Pointer> result;
    for (const auto& it : _segments) {
        if (_reader->GetBaseAddress()) {
            result.emplace(it.first, make_shared<InMemoryAiThetaSegment>(_reader, it.second));
        } else {
            result.emplace(it.first, make_shared<BufferedAiThetaSegment>(_reader, it.second));
        }
    }
    return result;
}

IndexContainer::Segment::Pointer CustomizedAiThetaContainer::fetch(const std::string& id,
                                                                   aitheta2::IndexContainer::FetchLevel level) const
{
    auto segment = get(id);

    if (segment == nullptr) {
        return nullptr;
    }
    if (level != aitheta2::IndexContainer::FetchLevel::HIGH) {
        return segment;
    }

    size_t regionSize = segment->data_size() + segment->padding_size();
    if (regionSize == 0) {
        LOG_WARN("segment[%s] is empty", id.c_str());
        return segment;
    }

    std::shared_ptr<char> data(new (std::nothrow) char[regionSize], [](char* p) { delete[] p; });
    if (data == nullptr) {
        LOG_ERROR("malloc [%lu]bytes failed", regionSize);
        return IndexContainer::Segment::Pointer();
    }

    size_t size = segment->fetch(/*offset*/ 0ul, data.get(), regionSize);
    if (size != regionSize) {
        LOG_ERROR("fetch data failed, expected[%lu] vs actual size[%lu]", regionSize, size);
        return IndexContainer::Segment::Pointer();
    }

    return Segment::Pointer(
        new InMemoryAiThetaSegment(data, segment->data_size(), segment->padding_size(), segment->data_crc()));
}

INDEX_FACTORY_REGISTER_CONTAINER(CustomizedAiThetaContainer);
} // namespace aitheta2
