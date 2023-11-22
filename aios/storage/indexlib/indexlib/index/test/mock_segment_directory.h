#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/segment_directory.h"

namespace indexlib { namespace index {

class MockSegmentDirectory : public merger::SegmentDirectory
{
public:
    MockSegmentDirectory(const file_system::DirectoryPtr& rootDirectory, const index_base::Version& version)
        : merger::SegmentDirectory(rootDirectory, version)
    {
    }

protected:
    void DoInit(bool hasSub, bool needDeletionMap) override {}

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MockSegmentDirectory);
}} // namespace indexlib::index
