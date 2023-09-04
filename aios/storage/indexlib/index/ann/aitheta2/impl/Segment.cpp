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
#include "indexlib/index/ann/aitheta2/impl/Segment.h"

#include "indexlib/index/ann/aitheta2/util/segment_data/ParallelMergeSegmentDataReader.h"

using namespace std;
using namespace autil::legacy;
using namespace indexlib::file_system;
namespace indexlibv2::index::ann {

SegmentDataWriterPtr Segment::GetSegmentDataWriter()
{
    if (_segmentDataWriter != nullptr) {
        return _segmentDataWriter;
    }
    _segmentDataWriter = std::make_shared<SegmentDataWriter>();
    if (!_segmentDataWriter->Init(_directory)) {
        _segmentDataWriter.reset();
    }
    return _segmentDataWriter;
}

SegmentDataReaderPtr Segment::GetSegmentDataReader()
{
    if (_segmentDataReader != nullptr) {
        return _segmentDataReader;
    }
    if (SegmentMeta::HasParallelMergeMeta(_directory)) {
        _segmentDataReader = make_shared<ParallelMergeSegmentDataReader>();
    } else {
        _segmentDataReader = make_shared<SegmentDataReader>();
    }
    if (!_segmentDataReader->Init(_directory, _isOnline)) {
        _segmentDataReader.reset();
    }
    return _segmentDataReader;
}

bool Segment::Close()
{
    if (_segmentDataReader != nullptr) {
        _segmentDataReader->Close();
    }
    if (_segmentDataWriter != nullptr) {
        _segmentDataWriter->Close();
    }
    _segmentMeta = SegmentMeta();
    _indexConfig = AithetaIndexConfig();

    AUTIL_LOG(INFO, "segment[%p] is closed", this);
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, Segment);

} // namespace indexlibv2::index::ann
