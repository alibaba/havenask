#include "indexlib/common_define.h"
#include <iostream>
#include "indexlib/common/exception.h"
#include "indexlib/storage/storage_define.h"
#include "indexlib/storage/segment_file.h"
#include "indexlib/storage/simple_attribute_formatter.h"
#include "indexlib/storage/default_cell_formatter.h"
#include "indexlib/storage/default_cell_comparator.h"
#include "indexlib/storage/default_block.h"

using namespace std;
using namespace apsara;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

int main(int argc, char** argv)
{
    if (argc != 2) 
    {
        cerr << "usage: " << argv[0] << " filePath" << endl;
        return -1;
    }
    string fileName = argv[1];
    static StorageOptions sStorageOptions;
    sStorageOptions.mCellComparator.reset(new DefaultCellComparator);
    sStorageOptions.mCellFormatter.reset(new DefaultCellFormatter);
    sStorageOptions.mBlockCreator.reset(new DefaultBlockCreator);
    sStorageOptions.mMaxBlockSize = 1 * 1024;
    sStorageOptions.mPanguHint.appName = "BIGFILE_APPNAME";
    sStorageOptions.mPanguHint.partName = "BIGFILE_PARTNAME";

    SegmentFile ycFile(fileName, &sStorageOptions);
    if (!ycFile.LoadIndex())
    {
        cerr << "Load segmentFile FAIL: " << fileName << endl;
        return -1;
    }

    uint32_t count = ycFile.GetCellCount();
    cout << "cellCount = " << count << endl;

    CellPtr cell;
    SimpleAttributeFormatter formatter;

    docid_t docId = 33907;
    docid_t beginDocId = docId / 1024 * 1024;

    while(ycFile.Next(cell))
    {
        docid_t key = formatter.DeserializeId(cell->GetKey());
        cout << "key: " << key << endl;
        if (key == beginDocId)
        {
            const int64_t* value = (const int64_t*)(cell->GetValue().data());
            for (int i = 0; i < 1024; ++i)
            {
                cout << "docId = " << key + i << ", value = " << value[i] << endl;
            }
            // cout << "docId = " << docId << ", value = " << value[docId - beginDocId] << endl;
        }
    }
    
    return 0;
}
