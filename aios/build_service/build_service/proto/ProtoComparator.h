#ifndef ISEARCH_BS_PROTOCOMPARATOR_H
#define ISEARCH_BS_PROTOCOMPARATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/Admin.pb.h"

namespace build_service {
namespace proto {

bool operator == (const BuildId &lft, const BuildId &rht);
bool operator != (const BuildId &lft, const BuildId &rht);
bool operator < (const BuildId &lft, const BuildId &rht);

bool operator == (const MergeTask &lft, const MergeTask &rht);
bool operator != (const MergeTask &lft, const MergeTask &rht);

bool operator == (const MergerTarget &lft, const MergerTarget &rht);
bool operator != (const MergerTarget &lft, const MergerTarget &rht);

bool operator == (const MergerCurrent &lft, const MergerCurrent &rht);
bool operator != (const MergerCurrent &lft, const MergerCurrent &rht);

bool operator == (const TaskTarget &lft, const TaskTarget &rht);
bool operator != (const TaskTarget &lft, const TaskTarget &rht);

bool operator == (const TaskCurrent &lft, const TaskCurrent &rht);
bool operator != (const TaskCurrent &lft, const TaskCurrent &rht);

bool operator == (const ProcessorTarget &lft, const ProcessorTarget &rht);
bool operator != (const ProcessorTarget &lft, const ProcessorTarget &rht);

bool operator == (const ProcessorCurrent &lft, const ProcessorCurrent &rht);
bool operator != (const ProcessorCurrent &lft, const ProcessorCurrent &rht);

bool operator == (const BuilderTarget &lft, const BuilderTarget &rht);
bool operator != (const BuilderTarget &lft, const BuilderTarget &rht);

bool operator == (const BuilderCurrent &lft, const BuilderCurrent &rht);
bool operator != (const BuilderCurrent &lft, const BuilderCurrent &rht);

bool operator == (const PBLocator &lft, const PBLocator &rht);
bool operator != (const PBLocator &lft, const PBLocator &rht);

bool operator == (const Range &lft, const Range &rht);
bool operator != (const Range &lft, const Range &rht);
bool operator < (const Range &lft, const Range &rht);

bool operator == (const PartitionId &lft, const PartitionId &rht);
bool operator != (const PartitionId &lft, const PartitionId &rht);
bool operator <(const PartitionId &lft, const PartitionId &rht);

bool operator == (const LongAddress &l, const LongAddress &r);
bool operator != (const LongAddress &l, const LongAddress &r);

bool operator == (const JobTarget &lft, const JobTarget &rht);
bool operator != (const JobTarget &lft, const JobTarget &rht);

bool operator == (const JobCurrent &lft, const JobCurrent &rht);
bool operator != (const JobCurrent &lft, const JobCurrent &rht);

bool operator == (const IndexInfo &lft, const IndexInfo &rht);
bool operator != (const IndexInfo &lft, const IndexInfo &rht);

bool operator == (const ProgressStatus &lft, const ProgressStatus &rht);
bool operator != (const ProgressStatus &lft, const ProgressStatus &rht);

}
}

#endif //ISEARCH_BS_PROTOCOMPARATOR_H
