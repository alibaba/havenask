#ifndef ISEARCH_BS_REUSEATTRIBUTEALTERFIELDMERGER_H
#define ISEARCH_BS_REUSEATTRIBUTEALTERFIELDMERGER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/custom_merger/AlterDefaultFieldMerger.h"

namespace build_service {
namespace custom_merger {

class ReuseAttributeAlterFieldMerger : public AlterDefaultFieldMerger
{
public:
    ReuseAttributeAlterFieldMerger();
    ~ReuseAttributeAlterFieldMerger();
    
private:
    ReuseAttributeAlterFieldMerger(const ReuseAttributeAlterFieldMerger &);
    ReuseAttributeAlterFieldMerger& operator=(const ReuseAttributeAlterFieldMerger &);

protected:
    void FillAttributeValue(
            const IE_NAMESPACE(partition)::PartitionIteratorPtr& partIter,
            const std::string& attrName, segmentid_t segmentId,
            const IE_NAMESPACE(partition)::AttributeDataPatcherPtr& attrPatcher) override;

public:
    static const std::string MERGER_NAME;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ReuseAttributeAlterFieldMerger);

}
}

#endif //ISEARCH_BS_REUSEATTRIBUTEALTERFIELDMERGER_H
