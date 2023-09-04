#include "indexlib/merger/test/demo_indexer.h"

#include "autil/legacy/jsonizable.h"
#include "indexlib/util/Exception.h"

using namespace std;

using namespace indexlib::document;
using namespace indexlib::index;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, DemoIndexer);

const std::string DemoIndexer::DATA_FILE_NAME = "demo_index_file";
const std::string DemoIndexer::META_FILE_NAME = "demo_meta_file";

DemoIndexer::DemoIndexer(const util::KeyValueMap& parameters)
    : Indexer(parameters)
    , mDataMap(AllocatorType(&mSimplePool))
    , mTotalDocStrLen(0)
{
}

DemoIndexer::~DemoIndexer() {}

bool DemoIndexer::DoInit(const IndexerResourcePtr& resource) { return true; }

bool DemoIndexer::Build(const std::vector<const Field*>& fieldVec, docid_t docId)
{
    string docStr = "";
    for (const auto& field : fieldVec) {
        if (field->GetFieldTag() == Field::FieldTag::RAW_FIELD) {
            auto rawField = static_cast<const IndexRawField*>(field);
            string fieldStr(rawField->GetData().data(), rawField->GetData().size());
            if (docStr == "") {
                docStr = fieldStr;
            } else {
                docStr = docStr + string(",") + fieldStr;
            }
        }
    }
    mTotalDocStrLen += docStr.size();
    bool ret = mDataMap.insert(make_pair(docId, docStr)).second;
    return ret;
}

bool DemoIndexer::Dump(const file_system::DirectoryPtr& indexDir)
{
    vector<DocItem> tempVector;
    tempVector.reserve(mDataMap.size());
    for (const auto& kv : mDataMap) {
        tempVector.push_back(make_pair(kv.first, kv.second));
    }
    try {
        string fileJsonStr = autil::legacy::ToJsonString(tempVector);
        indexDir->Store(DATA_FILE_NAME, fileJsonStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        IE_LOG(ERROR, "fail to store index file [%s], error: [%s]", DATA_FILE_NAME.c_str(), e.what());
        return false;
    }
    return true;
}

size_t DemoIndexer::EstimateInitMemoryUse(size_t lastSegmentDistinctTermCount) const
{
    return lastSegmentDistinctTermCount * 100; // for demonstrate
}

int64_t DemoIndexer::EstimateTempMemoryUseInDump() const { return sizeof(DocItem) * mDataMap.size() + mTotalDocStrLen; }

int64_t DemoIndexer::EstimateDumpFileSize() const
{
    return mDataMap.size() * sizeof(DocItem) * TO_JSON_EXPAND_FACTOR + mTotalDocStrLen;
}
}} // namespace indexlib::merger
