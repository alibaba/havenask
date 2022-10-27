#include "indexlib/index/normal/inverted_index/builtin_index/range/on_disk_range_index_iterator_creator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, OnDiskRangeIndexIteratorCreator);

OnDiskIndexIterator* OnDiskRangeIndexIteratorCreator::Create(
        const index_base::SegmentData& segData) const
{
    file_system::DirectoryPtr parentDirectory =
        segData.GetIndexDirectory(mParentIndexName, false);
    if (!parentDirectory)
    {
        return NULL;
    }

    file_system::DirectoryPtr indexDirectory =
        parentDirectory->GetDirectory(mIndexConfig->GetIndexName(), false);
    if (indexDirectory &&
        indexDirectory->IsExist(DICTIONARY_FILE_NAME))          
    {                                                           
        return (new OnDiskRangeIndexIterator(mIndexConfig, indexDirectory,                   
                        mPostingFormatOption, mIOConfig));      
    }       
    return NULL;                                                
}

IE_NAMESPACE_END(index);

