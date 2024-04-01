# -*- coding: utf-8 -*-
import click
import os
import shutil
import jinja2

from .common import *
from hape_libs.utils.template import CustomUndefined


class CustomizeContext(object):
    def __init__(self, conf_path, service_name_suffix, mode, has_vector_index, has_remote_nodes):
        self.conf_path = conf_path
        self.service_name_suffix = service_name_suffix
        self.mode = mode
        self.has_vector_index = has_vector_index
        self.has_remote_nodes = has_remote_nodes
        self.image = None
        
    def to_dict(self):
        return {
            "conf_path": self.conf_path,
            "service_name_suffix": self.service_name_suffix,
            "mode": self.mode,
            "has_vector_index": self.has_vector_index,
            "image": self.image,
            "has_remote_nodes": self.has_remote_nodes
        }
        
_customize_ctxs = [
    CustomizeContext("proc", "proc", "proc", False, False),
    CustomizeContext("default", "local", "docker", False, False),
    CustomizeContext("vector", "local", "docker",  True, False),
    CustomizeContext("vector-proc", "vector-proc", "proc",  True, False),
    CustomizeContext("remote", "remote", "docker", False, True),
    CustomizeContext("k8s", "k8s", "k8s", False, True)
]


def get_render_files(ctx, template_dir):
    files = []
    for dirpath, dirnames, filenames in os.walk(template_dir):
        for file_name in filenames:
            file_path = os.path.join(dirpath, file_name)
            relative_path = os.path.relpath(file_path, template_dir)
            files.append(relative_path)
            
    return files

@click.command(short_help='Customize hape config by case')
@click.option('-t', '--template', required=True, help="Template path of hape conf")
@click.option('-i', '--image', required=True, help="Image to render hape conf")
def customize(**kwargs):
    template_dir = kwargs['template']
    if not os.path.isdir(template_dir):
        raise ValueError("Argument template must be a directory")
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir), undefined = CustomUndefined)
    k8s_include = [
        "cluster_templates/appmaster/c2.yaml"
    ]
    for ctx in _customize_ctxs:
        ctx.image = kwargs['image']
        vars = ctx.to_dict()
        target_dir = os.path.join(template_dir, "..", ctx.conf_path)
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)
        render_files = get_render_files(ctx, template_dir)
            
        print("Render files: {}".format(render_files))
        print("Render variables: {}".format(vars))
        for file in render_files:
            template = env.get_template(file)
            ## if render will get undefine in kubeconfig variable
            skip_render = False
            if file in k8s_include:
                if ctx.mode == "k8s":
                    skip_render = True
                else:
                    continue
            if not skip_render:
                rendered_template = template.render(**{"customize":vars})
            else:
                rendered_template = open(os.path.join(template_dir, file)).read()
            target_file = os.path.join(target_dir, file)
            if not os.path.exists(os.path.dirname(target_file)):
                os.makedirs(os.path.dirname(target_file))
            with open(target_file, "w") as f:
                f.write(rendered_template)
        
