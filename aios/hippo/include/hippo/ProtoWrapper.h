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
#ifndef HIPPO_PROTOWRAPPER_H
#define HIPPO_PROTOWRAPPER_H

#include "hippo/DriverCommon.h"
#include "hippo/proto/Common.pb.h"

namespace hippo {

class ProtoWrapper
{
public:
    // covert proto message to hippo struct
    static void convert(const proto::AssignedSlot &in,
                        hippo::SlotInfo *out);
    static void convert(const proto::SlotId &in,
                        hippo::SlotId *out);
    static void convert(const proto::ProcessStatus &in,
                        hippo::ProcessStatus *out);
    static void convert(const proto::SlaveStatus &in,
                        hippo::SlaveStatus *out);
    static void convert(const proto::DataStatus &in,
                        hippo::DataStatus *out);
    static void convert(const proto::PackageStatus &in,
                        hippo::PackageStatus *out);
    static void convert(const proto::SlotResource &in,
                        hippo::SlotResource *out);
    static void convert(const proto::Resource &in,
                        hippo::Resource *out);

    static void convert(const proto::PackageInfo &in,
                        hippo::PackageInfo *out);
    static void convert(const proto::ProcessInfo &in,
                        hippo::ProcessInfo *out);
    static void convert(const proto::DataInfo &in,
                        hippo::DataInfo *out);
    static void convert(const proto::Parameter &in,
                        hippo::PairType *out);
    static void convert(const proto::Priority &in,
                        hippo::Priority *out);
    
    static void convert(const proto::ResourceRequest::AllocateMode &in,
                        hippo::ResourceAllocateMode *out);

    static void convert(const proto::ErrorInfo &in,
                        hippo::ErrorInfo *out);
    static void convert(const proto::SlotPreference &in,
                        hippo::SlotPreference *out);
    
    // convert hippo struct to proto message
    static void convert(const hippo::ProcessContext &in,
                        proto::ProcessLaunchContext *out);
    static void convert(const hippo::PackageInfo &in,
                        proto::PackageInfo *out);
    static void convert(const hippo::ProcessInfo &in,
                        proto::ProcessInfo *out);
    static void convert(const hippo::DataInfo &in,
                        proto::DataInfo *out);
    static void convert(const hippo::PairType &in,
                        proto::Parameter *out);
    static void convert(const std::vector<hippo::PackageInfo> &in,
                        std::vector<proto::PackageInfo> *out);
    static void convert(const std::map<std::string, hippo::PackageInfo> &in,
                        std::vector<proto::PackageInfo> *out);
    static void convert(const hippo::SlotResource &in,
                        proto::SlotResource *out);
    static void convert(const hippo::Resource &in,
                        proto::Resource *out);
    static void convert(const hippo::Priority &in,
                        proto::Priority *out);    

    static void convert(const hippo::ResourceAllocateMode &in,
                        proto::ResourceRequest::AllocateMode *out);
    
    static void convert(const hippo::ErrorInfo &in,
                        proto::ErrorInfo *out);

public:
    template <typename T1, typename T2>
    static void convert(const ::google::protobuf::RepeatedPtrField<T1> &in,
                        std::vector<T2> *out)
    {
        int count = in.size();
        out->resize(count);
        for (int i = 0; i < count; i++) {
            convert(in.Get(i), &(*out)[i]);
        }
    }

    template <typename T1, typename T2>
    static void convert(const ::google::protobuf::RepeatedPtrField<T1> &in,
                        std::vector<T2*> *out)
    {
        int count = in.size();
        out->resize(count);
        for (int i = 0; i < count; i++) {
            T2* tmp = new T2();
            convert(in.Get(i), tmp);
            (*out)[i] = tmp;
        }
    }

    template <typename T1, typename T2>
    static void convert(const std::vector<T1> &in,
                        ::google::protobuf::RepeatedPtrField<T2> *out)
    {
        for (size_t i = 0; i < in.size(); i++) {
            T2* t2 = out->Add();
            convert(in[i], t2);
        }
    }

private:
    ProtoWrapper();
    ~ProtoWrapper();
};

};

#endif //HIPPO_PROTOWRAPPER_H
