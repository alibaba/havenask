#ifndef __INDEXLIB_MOCK_SEGMENT_DIRECTORY_H
#define __INDEXLIB_MOCK_SEGMENT_DIRECTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(index);

class MockSegmentDirectory : public merger::SegmentDirectory
{
public:
    MockSegmentDirectory(const std::string& root,
                         const index_base::Version& version)
        : merger::SegmentDirectory(root, version)
    {}
protected:
    void DoInit(bool hasSub, bool needDeletionMap,
                const file_system::IndexlibFileSystemPtr& fileSystem) override
    {
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MockSegmentDirectory);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MOCK_SEGMENT_DIRECTORY_H
