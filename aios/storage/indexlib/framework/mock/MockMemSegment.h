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
#include "gmock/gmock.h"
#include <memory>
#include <string>
#include <vector>

#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/MemSegment.h"

namespace indexlib::framework {
class SegmentMetrics;
}

namespace indexlib::util {
class BuildResourceMetrics;
}

namespace indexlib::file_system {
class Directory;
class IFileSystem;
} // namespace indexlib::file_system

namespace indexlibv2::document {
class IDocumentBatch;
}

namespace indexlibv2::framework {

class Segment;
struct BuildResource;
class SegmentDumpItem;

class MockMemSegment : public MemSegment
{
public:
    MockMemSegment(const SegmentMeta& segmentMeta, Segment::SegmentStatus status) : MemSegment(segmentMeta)
    {
        SetSegmentMeta(segmentMeta);
        SetSegmentStatus(status);
    }

    MockMemSegment(segmentid_t segId, const std::shared_ptr<indexlib::file_system::IFileSystem>& fs,
                   Segment::SegmentStatus status)
        : MemSegment(SegmentMeta(segId))
    {
        assert(fs);
        auto tabletRoot = indexlib::file_system::Directory::Get(fs);
        std::string directoryName = "segment_" + std::to_string(segId) + "_level_0";
        auto segDir = tabletRoot->MakeDirectory(directoryName);
        SegmentMeta segmentMeta(segId);
        segmentMeta.segmentDir = segDir;
        segmentMeta.schema = std::make_shared<config::TabletSchema>();
        SetSegmentMeta(segmentMeta);

        SetSegmentStatus(status);
    }

    ~MockMemSegment() {}

    MOCK_METHOD(Status, Open, (const BuildResource&, indexlib::framework::SegmentMetrics*), (override));
    MOCK_METHOD(Status, Build, (document::IDocumentBatch*), (override));
    MOCK_METHOD(std::shared_ptr<indexlib::util::BuildResourceMetrics>, GetBuildResourceMetrics, (), (const, override));
    MOCK_METHOD(bool, NeedDump, (), (const, override));
    MOCK_METHOD(bool, IsDirty, (), (const, override));
    MOCK_METHOD((std::pair<Status, std::vector<std::shared_ptr<SegmentDumpItem>>>), CreateSegmentDumpItems, (),
                (override));
    MOCK_METHOD(void, EndDump, (), (override));
    MOCK_METHOD(void, Seal, (), (override));
    MOCK_METHOD(size_t, EvaluateCurrentMemUsed, (), (override));
};
} // namespace indexlibv2::framework
