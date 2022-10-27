#ifndef ISEARCH_KERNELREGISTER_H
#define ISEARCH_KERNELREGISTER_H

BEGIN_HA3_NAMESPACE(sql);

#define KERNEL_REGISTER_ADD_OUTPUT(def, name) def->add_outputs()->set_name(name);
#define KERNEL_REGISTER_ADD_INPUT(def, name) def->add_inputs()->set_name(name);
#define KERNEL_REGISTER_ADD_INPUT_OPTIONAL(def, name)   \
    {                                                   \
        auto input = def->add_inputs();                 \
        input->set_name(name);                          \
        input->set_type(navi::IT_OPTIONAL);             \
    }
#define KERNEL_REGISTER_ADD_INPUT_GROUP(def, name) def->add_input_groups()->set_name(name);
#define KERNEL_REGISTER_ADD_INPUT_GROUP_OPTIONAL(def, name)     \
    {                                                           \
        auto input = def->add_input_groups();                   \
        input->set_name(name);                                  \
        input->set_type(navi::IT_OPTIONAL);                     \
    }

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_KERNELREGISTER_H
