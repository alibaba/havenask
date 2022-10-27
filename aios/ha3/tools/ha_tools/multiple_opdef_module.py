# -*- coding: utf-8 -*-
import tensorflow as tf

class MultipleOpdefModule(object):
    def __init__(self, so_list):
        super(MultipleOpdefModule, self).__init__()
        self.opdef_modules = {so_file: tf.load_op_library(so_file) for so_file in so_list}
        self._check_op_name_conflict()

    def _check_op_name_conflict(self):
        opname_reverse_dict = {}
        for so_name, module in self.opdef_modules.iteritems():
            for attr_name in dir(module):
                if attr_name.endswith('_op'):
                    old_so_name = opname_reverse_dict.get(attr_name)
                    assert not old_so_name, 'duplicate op name `{}`, from `{}` and from `{}`'.format(
                        attr_name, old_so_name, so_name)

    def __getattr__(self, attr_name):
        if attr_name.endswith('_op'):
            for module in self.opdef_modules.itervalues():
                if hasattr(module, attr_name):
                    return getattr(module, attr_name)
        raise AttributeError("attribute `{}` not found".format(attr_name));
