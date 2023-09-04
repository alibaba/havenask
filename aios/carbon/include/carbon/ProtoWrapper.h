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
#ifndef CARBON_PROTOWRAPPER_H
#define CARBON_PROTOWRAPPER_H

#include "carbon/proto/Status.pb.h"
#include "carbon/Status.h"

namespace carbon {

class ProtoWrapper {
 public:
    // covert proto message to hippo struct
    static void convert(const proto::GroupStatus &in,
                        carbon::GroupStatus *out);
    static void convert(const proto::RoleStatusValue &in,
                        carbon::RoleStatus *out);
    static void convert(const proto::ReplicaNodeStatus &in,
                        carbon::ReplicaNodeStatus *out);
    static void convert(const proto::WorkerNodeStatus &in,
                        carbon::WorkerNodeStatus *out);
    static void convert(const proto::SlotStatus &in,
                        carbon::SlotStatus *out);
    static void convert(const proto::ServiceStatus &in,
                        carbon::ServiceInfo *out);
    static void convert(const proto::HealthInfo &in,
                        carbon::HealthInfo *out);
    static void convert(const proto::SlotInfo &in,
                        hippo::SlotInfo *out);
    static void convert(const proto::GlobalPlan &in,
                        carbon::GlobalPlan *out);
    static void convert(const proto::VersionedPlan &in,
                        carbon::VersionedPlan *out);
    static void convert(const proto::ResourcePlan &in,
                        carbon::ResourcePlan *out);
    static void convert(const proto::SlotResource &in,
                        hippo::SlotResource *out);

 public:
    // basic types
    inline static void convert(const std::string &in, std::string *out) {
        *out = in;
    }
    inline static void convert(const bool &in, bool *out) {
        *out = in;
    }
    inline static void convert(const int &in, int *out) {
        *out = in;
    }
    inline static void convert(const unsigned int &in, unsigned int *out) {
        *out = in;
    }
    inline static void convert(const short &in, short *out) {
        *out = in;
    }
    inline static void convert(const unsigned short &in, unsigned short *out) {
        *out = in;
    }
    inline static void convert(const long &in, long *out) {
        *out = in;
    }
    inline static void convert(const unsigned long &in, unsigned long *out) {
        *out = in;
    }
    inline static void convert(const float &in, float *out) {
        *out = in;
    }
    inline static void convert(const double &in, double *out) {
        *out = in;
    }
    inline static void convert(const long double &in, long double *out) {
        *out = in;
    }

    template <typename T1, typename T2>
    inline static void convert(const ::google::protobuf::RepeatedPtrField<T1> &in, std::vector<T2> *out) {
        int count = in.size();
        out->resize(count);
        for (int i = 0; i < count; i++) {
            convert(in.Get(i), &(*out)[i]);
        }
    }

    template <typename T1, typename T2>
    inline static void convert(const ::google::protobuf::RepeatedPtrField<T1> &in, std::vector<T2 *> *out) {
        int count = in.size();
        out->resize(count);
        for (int i = 0; i < count; i++) {
            T2 *tmp = new T2();
            convert(in.Get(i), tmp);
            (*out)[i] = tmp;
        }
    }

    template <typename T1, typename T2>
    inline static void convert(const ::google::protobuf::RepeatedPtrField<T1> &in, std::map<std::string, T2*> *out) {
        int count = in.size();
        for (int i = 0; i < count; i++) {
            T2 *tmp = new T2();
            convert(in.Get(i).value(), tmp);
            (*out)[in.Get(i).key()] = tmp;
        }
    }

    template <typename T1, typename T2>
    inline static void convert(const ::google::protobuf::RepeatedPtrField<T1> &in, std::map<std::string, T2> *out) {
        int count = in.size();
        for (int i = 0; i < count; i++) {
            T2 tmp;
            convert(in.Get(i).value(), &tmp);
            (*out)[in.Get(i).key()] = tmp;
        }
    }

    template <typename T1, typename T2>
    inline static void convert(const ::google::protobuf::Map<std::string, T1> &in, std::map<std::string, T2*> *out) {
        for(auto it = in.cbegin(); it != in.cend(); ++it)
        {
            T2 *tmp = new T2();
            convert(it->second, tmp);
            (*out)[it->first] = tmp;
        }
    }

    template <typename T1, typename T2>
    inline static void convert(const ::google::protobuf::Map<std::string, T1> &in, std::map<std::string, T2> *out) {
        for(auto it = in.cbegin(); it != in.cend(); ++it)
        {
            T2 tmp;
            convert(it->second, &tmp);
            (*out)[it->first] = tmp;
        }
    }

 private:
    ProtoWrapper();
    ~ProtoWrapper();
};

};  // namespace hippo

#endif  // HIPPO_PROTOWRAPPER_H
