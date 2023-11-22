/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/proto/ProtoComparator.h"

#include <iosfwd>
#include <string>

using namespace std;

namespace build_service { namespace proto {

bool operator==(const BuildId& lft, const BuildId& rht)
{
    return lft.datatable() == rht.datatable() && lft.generationid() == rht.generationid() &&
           lft.appname() == rht.appname();
}

bool operator!=(const BuildId& lft, const BuildId& rht) { return !(lft == rht); }

bool operator<(const BuildId& lft, const BuildId& rht)
{
    if (lft.datatable() != rht.datatable()) {
        return lft.datatable() < rht.datatable();
    }
    if (lft.generationid() != rht.generationid()) {
        return lft.generationid() < rht.generationid();
    }
    return lft.appname() < rht.appname();
}

bool operator==(const MergeTask& lft, const MergeTask& rht)
{
    return lft.timestamp() == rht.timestamp() && lft.mergeconfigname() == rht.mergeconfigname();
}

bool operator!=(const MergeTask& lft, const MergeTask& rht) { return !(lft == rht); }

bool operator==(const MergerTarget& lft, const MergerTarget& rht)
{
    return lft.configpath() == rht.configpath() && lft.mergestep() == rht.mergestep() &&
           lft.mergetask() == rht.mergetask() && lft.partitioncount() == rht.partitioncount() &&
           lft.buildparallelnum() == rht.buildparallelnum() && lft.mergeparallelnum() == rht.mergeparallelnum() &&
           lft.optimizemerge() == rht.optimizemerge() && lft.alignedversionid() == rht.alignedversionid() &&
           lft.suspendtask() == rht.suspendtask() && lft.workerpathversion() == rht.workerpathversion();
}

bool operator!=(const MergerTarget& lft, const MergerTarget& rht) { return !(lft == rht); }

bool operator==(const TaskTarget& lft, const TaskTarget& rht)
{
    return lft.configpath() == rht.configpath() && lft.suspendtask() == rht.suspendtask() &&
           lft.targetdescription() == rht.targetdescription() && lft.targettimestamp() == rht.targettimestamp();
}
bool operator!=(const TaskTarget& lft, const TaskTarget& rht) { return !(lft == rht); }

bool operator==(const AgentTarget& lft, const AgentTarget& rht)
{
    bool ret = lft.configpath() == rht.configpath() && lft.suspendtask() == rht.suspendtask() &&
               lft.isglobalagent() == rht.isglobalagent();
    if (!ret) {
        return false;
    }

    if (lft.globalconfigs_size() != rht.globalconfigs_size()) {
        return false;
    }
    for (int i = 0; i < lft.globalconfigs_size(); i++) {
        if (lft.globalconfigs(i) != rht.globalconfigs(i)) {
            return false;
        }
    }
    if (lft.targetroles_size() != rht.targetroles_size()) {
        return false;
    }
    for (int i = 0; i < lft.targetroles_size(); i++) {
        if (lft.targetroles(i).rolename() != rht.targetroles(i).rolename()) {
            return false;
        }
        if (lft.targetroles(i).processinfo() != rht.targetroles(i).processinfo()) {
            return false;
        }
        if (lft.targetroles(i).configpath() != rht.targetroles(i).configpath()) {
            return false;
        }
    }
    return true;
}

bool operator!=(const AgentTarget& lft, const AgentTarget& rht) { return !(lft == rht); }

bool operator==(const AgentCurrent& lft, const AgentCurrent& rht)
{
    return lft.targetstatus() == rht.targetstatus() && lft.status() == rht.status() &&
           lft.longaddress() == rht.longaddress() && lft.issuspended() == rht.issuspended() &&
           lft.protocolversion() == rht.protocolversion() && lft.configpath() == rht.configpath();
}

bool operator!=(const AgentCurrent& lft, const AgentCurrent& rht) { return !(lft == rht); }

bool operator!=(const TaskCurrent& lft, const TaskCurrent& rht) { return !(lft == rht); }

bool operator==(const PBLocator& lft, const PBLocator& rht)
{
    return lft.sourcesignature() == rht.sourcesignature() && lft.checkpoint() == rht.checkpoint();
}

bool operator!=(const PBLocator& lft, const PBLocator& rht) { return !(lft == rht); }

bool operator==(const ProcessorTarget& lft, const ProcessorTarget& rht)
{
    return lft.configpath() == rht.configpath() && lft.datadescription() == rht.datadescription() &&
           lft.startlocator() == rht.startlocator() && lft.stoptimestamp() == rht.stoptimestamp() &&
           lft.suspendtimestamp() == rht.suspendtimestamp() && lft.suspendtask() == rht.suspendtask();
}

bool operator!=(const ProcessorTarget& lft, const ProcessorTarget& rht) { return !(lft == rht); }

bool operator==(const ProcessorCurrent& lft, const ProcessorCurrent& rht)
{
    return lft.targetstatus() == rht.targetstatus() && lft.datasourcefinish() == rht.datasourcefinish() &&
           lft.stoplocator() == rht.stoplocator() && lft.status() == rht.status() &&
           lft.longaddress() == rht.longaddress() && lft.progressstatus() == rht.progressstatus() &&
           lft.issuspended() == rht.issuspended() && lft.protocolversion() == rht.protocolversion() &&
           lft.configpath() == rht.configpath();
}

bool operator!=(const ProcessorCurrent& lft, const ProcessorCurrent& rht) { return !(lft == rht); }

bool operator==(const BuilderTarget& lft, const BuilderTarget& rht)
{
    return lft.configpath() == rht.configpath() && lft.starttimestamp() == rht.starttimestamp() &&
           lft.stoptimestamp() == rht.stoptimestamp() && lft.suspendtask() == rht.suspendtask() &&
           lft.workerpathversion() == rht.workerpathversion();
}

bool operator!=(const BuilderTarget& lft, const BuilderTarget& rht) { return !(lft == rht); }

bool operator==(const BuilderCurrent& lft, const BuilderCurrent& rht)
{
    return lft.targetstatus() == rht.targetstatus() && lft.status() == rht.status() &&
           lft.longaddress() == rht.longaddress() && lft.progressstatus() == rht.progressstatus() &&
           lft.issuspended() == rht.issuspended() && lft.protocolversion() == rht.protocolversion() &&
           lft.configpath() == rht.configpath();
}

bool operator!=(const BuilderCurrent& lft, const BuilderCurrent& rht) { return !(lft == rht); }

bool operator==(const Range& lft, const Range& rht) { return lft.from() == rht.from() && lft.to() == rht.to(); }

bool operator!=(const Range& lft, const Range& rht) { return !(lft == rht); }

bool operator<(const Range& lft, const Range& rht)
{
    if (lft.from() != rht.from()) {
        return lft.from() < rht.from();
    }
    return lft.to() < rht.to();
}

bool operator==(const PartitionId& lft, const PartitionId& rht)
{
    bool ret = lft.role() == rht.role() && lft.step() == rht.step() && lft.range() == rht.range() &&
               lft.buildid() == rht.buildid() && lft.mergeconfigname() == rht.mergeconfigname() &&
               lft.taskid() == rht.taskid() && lft.backupid() == rht.backupid();
    if (!ret) {
        return false;
    }
    if (lft.clusternames_size() != rht.clusternames_size()) {
        return false;
    }
    for (int i = 0; i < lft.clusternames_size(); i++) {
        if (lft.clusternames(i) != rht.clusternames(i)) {
            return false;
        }
    }
    return true;
}

bool operator!=(const PartitionId& lft, const PartitionId& rht) { return !(lft == rht); }

bool operator<(const PartitionId& lft, const PartitionId& rht)
{
    if (lft.role() != rht.role()) {
        return lft.role() < rht.role();
    }
    if (lft.step() != rht.step()) {
        return lft.step() < rht.step();
    }
    if (lft.range() != rht.range()) {
        return lft.range() < rht.range();
    }
    if (lft.buildid() != rht.buildid()) {
        return lft.buildid() < rht.buildid();
    }
    if (lft.mergeconfigname() != rht.mergeconfigname()) {
        return lft.mergeconfigname() < rht.mergeconfigname();
    }
    if (lft.clusternames_size() != rht.clusternames_size()) {
        return lft.clusternames_size() < rht.clusternames_size();
    }
    for (int i = 0; i < lft.clusternames_size(); i++) {
        if (lft.clusternames(i) != rht.clusternames(i)) {
            return lft.clusternames(i) < rht.clusternames(i);
        }
    }
    if (lft.taskid() != rht.taskid()) {
        return lft.taskid() < rht.taskid();
    }
    if (lft.backupid() != rht.backupid()) {
        return lft.backupid() < rht.backupid();
    }
    return false;
}

bool operator==(const ErrorInfo& lft, const ErrorInfo& rht)
{
    return lft.errorcode() == rht.errorcode() && lft.errormsg() == rht.errormsg() && lft.errortime() == rht.errortime();
}

bool operator!=(const ErrorInfo& lft, const ErrorInfo& rht) { return !(lft == rht); }

bool operator==(const LongAddress& l, const LongAddress& r)
{
    return l.ip() == r.ip() && l.arpcport() == r.arpcport() && l.httpport() == r.httpport() && l.pid() == r.pid() &&
           l.starttimestamp() == r.starttimestamp();
}

bool operator!=(const LongAddress& l, const LongAddress& r) { return !(l == r); }

bool operator==(const TaskCurrent& lft, const TaskCurrent& rht)
{
    bool ret = lft.reachedtarget() == rht.reachedtarget() && lft.status() == rht.status() &&
               lft.longaddress() == rht.longaddress() && lft.progressstatus() == rht.progressstatus() &&
               lft.protocolversion() == rht.protocolversion() && lft.configpath() == rht.configpath() &&
               lft.statusdescription() == rht.statusdescription();

    if (!ret) {
        return false;
    }

    if (lft.errorinfos_size() != rht.errorinfos_size()) {
        return false;
    }
    for (int i = 0; i < lft.errorinfos_size(); ++i) {
        if (lft.errorinfos(i) != rht.errorinfos(i)) {
            return false;
        }
    }
    return true;
}

bool operator==(const MergerCurrent& lft, const MergerCurrent& rht)
{
    bool ret = lft.targetstatus() == rht.targetstatus() && lft.indexinfo() == rht.indexinfo() &&
               lft.longaddress() == rht.longaddress() && lft.targetversionid() == rht.targetversionid() &&
               lft.progressstatus() == rht.progressstatus() && lft.issuspended() == rht.issuspended() &&
               lft.protocolversion() == rht.protocolversion() && lft.configpath() == rht.configpath();

    if (!ret) {
        return false;
    }

    if (lft.errorinfos_size() != rht.errorinfos_size()) {
        return false;
    }
    for (int i = 0; i < lft.errorinfos_size(); ++i) {
        if (lft.errorinfos(i) != rht.errorinfos(i)) {
            return false;
        }
    }
    return true;
}

bool operator!=(const MergerCurrent& lft, const MergerCurrent& rht) { return !(lft == rht); }

bool operator==(const JobTarget& lft, const JobTarget& rht)
{
    return lft.configpath() == rht.configpath() && lft.datasourceid() == rht.datasourceid() &&
           lft.datadescription() == rht.datadescription() && lft.suspendtask() == rht.suspendtask();
}

bool operator!=(const JobTarget& lft, const JobTarget& rht) { return !(lft == rht); }

bool operator==(const JobCurrent& lft, const JobCurrent& rht)
{
    bool ret = lft.targetstatus() == rht.targetstatus() && lft.status() == rht.status() &&
               lft.currentlocator() == rht.currentlocator() && lft.longaddress() == rht.longaddress() &&
               lft.issuspended() == rht.issuspended() && lft.protocolversion() == rht.protocolversion() &&
               lft.configpath() == rht.configpath();

    if (!ret) {
        return false;
    }

    if (lft.errorinfos_size() != rht.errorinfos_size()) {
        return false;
    }
    for (int i = 0; i < lft.errorinfos_size(); ++i) {
        if (lft.errorinfos(i) != rht.errorinfos(i)) {
            return false;
        }
    }
    return true;
}

bool operator!=(const JobCurrent& lft, const JobCurrent& rht) { return !(lft == rht); }

bool operator==(const IndexInfo& lft, const IndexInfo& rht)
{
    return lft.clustername() == rht.clustername() && lft.range() == rht.range() &&
           lft.indexversion() == rht.indexversion() && lft.versiontimestamp() == rht.versiontimestamp() &&
           lft.indexsize() == rht.indexsize() && lft.starttimestamp() == rht.starttimestamp() &&
           lft.finishtimestamp() == rht.finishtimestamp() && lft.schemaversion() == rht.schemaversion();
}

bool operator!=(const IndexInfo& lft, const IndexInfo& rht) { return !(lft == rht); }

bool operator==(const ProgressStatus& lft, const ProgressStatus& rht)
{
    return lft.starttimestamp() == rht.starttimestamp() && lft.reporttimestamp() == rht.reporttimestamp() &&
           lft.progress() == rht.progress() && lft.startpoint() == rht.startpoint() &&
           lft.locatortimestamp() == rht.locatortimestamp();
}

bool operator!=(const ProgressStatus& lft, const ProgressStatus& rht) { return !(lft == rht); }

}} // namespace build_service::proto
