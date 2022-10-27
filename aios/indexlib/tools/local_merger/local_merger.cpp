#include <iostream>
#include <string>
#include <fstream>
#include "indexlib/partition/index_partition.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/index/optimize_merge_strategy.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

using namespace autil::legacy::json;
using namespace autil::legacy;
using namespace std;

int main(int argc, char* argv[]) 
{
    if (argc < 3)
    {
        cout << "Usage: ./local_merger INDEX_DIR SCHEMA_PATH [./INDEX_PARTITION_OPTIONS]" << endl;
        return -1;
    }

    string indexDir = string(argv[1]);
    string schemaPath = string(argv[2]);
    string appName = "BIGFILE_APPNAME";
    string partName = "BIGFILE_PARTNAME";

    IndexPartitionOptions options;
    if (argc == 4)
    {
        ifstream jsonFileReader(argv[3]);
        if(!jsonFileReader){
            cerr<<"Can't open the file "<<argv[3]<<endl;
            return -1;
        }
        string line,jsonOptionsContent;
        while(getline(jsonFileReader,line)){
            jsonOptionsContent+=line;
        }
        jsonFileReader.close();
        autil::legacy::FromJsonString(options, jsonOptionsContent);
        cout<<"Init IndexPartitionOptions from json."<<endl;
    }

    IndexPartition partition;
    IndexPartitionReaderPtr reader;
    IndexPartitionMergerPtr merger;

    // parse schema
    cout << "parsing " << schemaPath << endl;
    ifstream in(schemaPath.data());
    if (!in)
    {
        cout << "cannot open " << schemaPath << endl;
        return -1;
    }
    string line;
    string jsonString;
    while(getline(in, line)) 
    {
        jsonString += line;
    }
    in.close();

    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr indexSchema(new IndexPartitionSchema);
    FromJson(*indexSchema, any);

    cout << "Opening partition" << endl;
    partition.Open(indexDir, IndexPartition::APPEND | IndexPartition::READ, indexSchema, options);
    merger = partition.GetWriter()->GetMerger();

    MergeTask task;                                                                                                                        
    OptimizeMergeStrategy strategy;                                                                                                         
    MergeTaskParam param(indexDir, partition.GetReader()->GetSegmentInfos(), 0);
    task = strategy.CreateMergeTask(param);

    cout << "Start merging..." << endl;
    merger->Merge(task);

    cout << "-------- end -------" << endl;
    return 0;
}
