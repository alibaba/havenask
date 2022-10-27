#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/util/key_hasher_typed.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexReader);

IndexReader::IndexReader() 
{
}

IndexReader::~IndexReader()
{
}

bool IndexReader::GenFieldMapMask(const string &indexName,
                                  const vector<string>& termFieldNames,
                                  fieldmap_t &targetFieldMap)
{
    //TODO: check option
    targetFieldMap = 0;
    config::PackageIndexConfigPtr expackIndexConfig =
        tr1::dynamic_pointer_cast<config::PackageIndexConfig>(mIndexConfig);
    if (!expackIndexConfig)
    {
        return false;
    }
    
    int32_t fieldIdxInPack = -1;
    for (size_t i = 0; i < termFieldNames.size(); ++i)
    {
        // set expack field idx: mIndexConfig
        fieldIdxInPack = expackIndexConfig->GetFieldIdxInPack(termFieldNames[i]);
        if (fieldIdxInPack == -1)
        {
            IE_LOG(WARN, "GenFieldMapMask failed: [%s] not in expack field!", 
                   termFieldNames[i].c_str());
            return false;
        }
        targetFieldMap |= (fieldmap_t)(1 << fieldIdxInPack);
    }
    return true;
}

std::vector<PostingIterator*> IndexReader::BatchLookup(
        const std::vector<LookupContext>& contexts, autil::mem_pool::Pool* pool)
{
    std::vector<future_lite::Future<PostingIterator*>> futures;
    futures.reserve(contexts.size());
    for (const auto& context : contexts)
    {
        futures.push_back(LookupAsync(context.term, context.statePoolSize, context.type, pool));
    }
    auto f = future_lite::collectAll(futures.begin(), futures.end());
    f.wait();
    std::vector<PostingIterator*> ret;
    ret.reserve(futures.size());
    std::vector<future_lite::Try<PostingIterator*>>& results = f.value();
    for (auto&& t : results)
    {
        ret.push_back(t.value());
    }
    return ret;
}

IE_NAMESPACE_END(index);
