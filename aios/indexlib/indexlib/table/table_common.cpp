#include "indexlib/table/table_common.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);

void MergeSegmentDescription::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("target_segment_docCount", docCount);    
    json.Jsonize("use_specified_dp_file", useSpecifiedDeployFileList);
    json.Jsonize("deploy_file_list", deployFileList); 
}

IE_NAMESPACE_END(table);

