#ifndef __INDEXLIB_OFFLINE_SEGMENT_DIRECTORY_H
#define __INDEXLIB_OFFLINE_SEGMENT_DIRECTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/config/configurator_define.h"

IE_NAMESPACE_BEGIN(index_base);

class OfflineSegmentDirectory : public SegmentDirectory
{
public:
    OfflineSegmentDirectory(bool enableRecover = false,
                            config::RecoverStrategyType recoverType
                            = config::RecoverStrategyType::RST_SEGMENT_LEVEL);
    OfflineSegmentDirectory(const OfflineSegmentDirectory& other);
    ~OfflineSegmentDirectory();
public:

protected:
    void DoInit(const file_system::DirectoryPtr& directory) override;

protected:
    bool mEnableRecover;
    config::RecoverStrategyType mRecoverType;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineSegmentDirectory);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_OFFLINE_SEGMENT_DIRECTORY_H
