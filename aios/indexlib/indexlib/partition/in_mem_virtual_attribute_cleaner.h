#ifndef __INDEXLIB_IN_MEM_VIRTUAL_ATTRIBUTE_CLEANER_H
#define __INDEXLIB_IN_MEM_VIRTUAL_ATTRIBUTE_CLEANER_H

#include <tr1/memory>
#include <fslib/common/common_type.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/executor.h"

DECLARE_REFERENCE_CLASS(partition, VirtualAttributeContainer);
DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(partition);

class InMemVirtualAttributeCleaner : public common::Executor
{
public:
    InMemVirtualAttributeCleaner(
            const ReaderContainerPtr& readerContainer,
            const VirtualAttributeContainerPtr& attrContainer,
            const file_system::DirectoryPtr& directory);

    ~InMemVirtualAttributeCleaner();

public:
    void Execute() override;
private:
    void ClearAttributeSegmentFile(const fslib::FileList& segments,
                                   const std::vector<std::pair<std::string,bool>>& attributeNames,
                                   const file_system::DirectoryPtr& directory);
private:
    ReaderContainerPtr mReaderContainer;
    VirtualAttributeContainerPtr mVirtualAttrContainer;
    file_system::DirectoryPtr mDirectory;
    bool mLocalOnly;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemVirtualAttributeCleaner);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_IN_MEM_VIRTUAL_ATTRIBUTE_CLEANER_H
