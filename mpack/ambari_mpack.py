#!/usr/bin/python3

import glob
import os
import json
import time
import xml.etree.ElementTree as ET

from ansible.module_utils.basic import AnsibleModule
from ambariclient.client import Ambari
import ambariclient.exceptions as ambari_exceptions

ANSIBLE_METADATA = {
    "metadata_version": "1.0",
    "supported_by": "community",
    "status": ["preview"],
    "version": "1.1.0"
}


def build_module():
    fields = {
        "login": {"required": False, "type": "str", "no_log": True, "default": "admin"},
        "password": {"required": False, "type": "str", "no_log": True, "default": "admin"},
        "host": {"required": False, "type": "str", "default": "localhost"},
        "port": {"required": False, "type": "str", "default": "8080"},
        "cluster_name": {"required": True, "type": "str"},
        "service": {"required": False, "type": "str"},
        "config_path": {"required": False, "type": "str"},
        "components": {"required": False, "type": "list"},
        "extra": {"required": False, "type": "list"},
        "count": {"required": False, "type": "int"},
        "state": {
            "default": "present",
            "choices": ['present', 'absent', 'started'],
            "type": 'str'
        }
    }

    mutually_exclusive = []
    module = AnsibleModule(
        argument_spec=fields,
        mutually_exclusive=mutually_exclusive,
        supports_check_mode=True
    )

    return module

def read_xml_data_to_map(path):
    configurations = {}
    properties_attributes = {}
    tree = ET.parse(path)
    root = tree.getroot()
    for properties in root.iter('property'):
        name = properties.find('name')
        value = properties.find('value')
        final = properties.find('final')

        if name is not None:
            name_text = name.text if name.text else ""
        else:
            continue

        if value is not None:
            value_text = value.text if value.text else ""
        else:
            value_text = ""

        if final is not None:
            final_text = final.text if final.text else ""
            properties_attributes[name_text] = final_text

        configurations[name_text] = value_text
    return configurations

class Mpack:
    def __init__(self, service_name, components, count, config_path,
                 extra_config, cluster_name, api_client, module, no_wait=False, **kwargs):
        self.service_name = service_name
        self.module = module
        self.cluster_name = cluster_name
        self.api_client = api_client
        self.cluster_version = self._get_cluster_version()
        self.no_wait = no_wait
        self.changed = False
        self.components = components
        self._update()
        self.config_path = config_path
        self.component_hosts_count = count
        self.extra_config = extra_config

    def _get_cluster_version(self):
        return self.api_client.version

    def _update(self):
        self.status = self._get_status()

    def _get_status(self):
        try:
            return self.api_client.clusters(self.cluster_name).services(self.service_name).state
        except (ambari_exceptions.BadRequest, ambari_exceptions.NotFound):
            return "ABSENT"

    def _get_services_list(self):
        return self.api_client.clusters(self.cluster_name).services

    def started(self):
        if self.status != "INSTALLED":
            self.installed()
        if self.status != "STARTED":
            self._stop_service()
            self._start_service()

    def _start_service(self):
        self.api_client.clusters(self.cluster_name).services(self.service_name).start().wait()
        self._update()
        self.changed = True

    def installed(self):
        if self.status == "ABSENT":
            self._create_service()
        if self.status == "UNKNOWN":
            for component in self.components:
                self._add_service_component_to_service(component)

            for config_name in glob.glob(f"{self.config_path}/*.xml"):
                config_type = os.path.splitext(os.path.basename(config_name))[0]
                self._add_service_configuration(config_type, json.dumps(read_xml_data_to_map(config_name)))

            if self.extra_config is not None:
                for config in self.extra_config:
                    self._update_configuration(config[0], config[1], config[2])
            for component in self.components:
                if self.component_hosts_count != 0:
                    index = 0
                    for host in self.api_client.clusters(self.cluster_name).hosts:
                        self._add_service_component_to_host(component, host.host_name)
                        index += 1
                        if index == self.component_hosts_count:
                            break
            self._restart_stale_config_components()
        self._update()


    def _add_service_component_to_host(self, component, host_name):
        request_url = self.api_client.clusters(self.cluster_name).hosts.url + f"?Hosts/host_name={host_name}"

        self.api_client.post(request_url, content_type="application/json", data={
                "RequestInfo": {
                    "context": '',
                    "operation_level": {
                        "level": "HOST",
                        "cluster_name": self.cluster_name,
                        "host_name": host_name,
                        "service_name": self.service_name
                    }
                },
                "Body": {
                    "host_components": [
                        {
                            "HostRoles": {
                                "component_name": component
                            }
                        }
                    ]
                }
            })
        self.api_client.clusters(self.cluster_name).hosts(host_name).components(component).install().wait()


    def _update_configuration(self, config_type, propertie_key, propertie_value):
        new_tag = str(time.time())
        current_tag = self.api_client.clusters(self.cluster_name).desired_configs[config_type]['tag']
        current_item = {}

        for config_item in self.api_client.clusters(self.cluster_name).configurations(config_type).items().to_dict():
            if config_item['tag'] == current_tag:
                current_item = config_item['items'][0]
                break

        new_item = {'Clusters': {'desired_config': [{'type': config_type, 'tag': new_tag}]}}

        for key, value in current_item.items():
            if key.upper() not in ('VERSION', 'CONFIG', 'TYPE', 'TAG', 'HREF'):
                new_item['Clusters']['desired_config'][0][key] = value

        if propertie_key in new_item['Clusters']['desired_config'][0]['properties'] and \
                new_item['Clusters']['desired_config'][0]['properties'][propertie_key] != '':
                    if propertie_value not in new_item['Clusters']['desired_config'][0]['properties'][propertie_key]:
                        propertie_value = new_item['Clusters']['desired_config'][0]['properties'][propertie_key] + propertie_value
                    else:
                        propertie_value = new_item['Clusters']['desired_config'][0]['properties'][propertie_key]

        new_item['Clusters']['desired_config'][0]['properties'].update({propertie_key: propertie_value})

        self.api_client.put(self.api_client.clusters(self.cluster_name).url,
                            content_type="application/json",
                            data=json.dumps(new_item))
        self.changed = True

    def _add_service_configuration(self, config_type, properties):
        data = '{"Clusters":{"desired_config":['
        data = data + "{" + f'"type": "{config_type}", "properties" : {properties}' + "},"
        data = data[:-1] + ']}}'.replace('\'', '"')
        self.api_client.put(self.api_client.clusters(self.cluster_name).url,
                            content_type="application/json",
                            data=data)
        self.changed = True

    def _add_service_component_to_service(self, component):
        component_url = \
            self.api_client.clusters(self.cluster_name).services(self.service_name).components(component).url
        self.api_client.post(component_url, content_type="application/json")
        self.changed = True

    def _create_service(self):
        self.api_client.post(self.api_client.clusters(self.cluster_name).services(self.service_name).url,
                             content_type="application/json")
        self._update()
        self.changed = True

    def _restart_stale_config_components(self):
        self.api_client.clusters(self.cluster_name).restart_stale_config_components()
        self.api_client.post(self.api_client.clusters(self.cluster_name).requests.url, data={
            "RequestInfo": {
                "context": ('Restart all required services'),
                "operation_level": "host_component",
                "command": "RESTART"
            },
            "Requests/resource_filters": [{
                "hosts_predicate": "HostRoles/stale_configs=true&HostRoles/cluster_name=" + self.cluster_name
            }],
        })
        self.changed = True

    def deleted(self):
        if self.status != "ABSENT":
            self._stop_service()
            self._delete_service()

    def _stop_service(self):
        try:
            self.api_client.clusters(self.cluster_name).services(self.service_name).stop().wait()
            self.changed = True
        except Exception:
            self._update()

    def _delete_service(self):
        self.api_client.delete(self.api_client.clusters(self.cluster_name).services(self.service_name).url,
                             content_type="application/json")
        self._update()
        self.changed = True

    def meta(self):
        meta = {
            "product": self.service_name,
            "version": self.cluster_version
        }
        return meta

    def __repr__(self):
        return f'Mpack(name="{self.service_name}", version="{self.cluster_version}",\
                       cluster_name="{self.cluster_name}", api_client={self.api_client},\
                       status={self.status})'

    def __str__(self):
        return f"name: {self.service_name}, version: {self.cluster_version}"


def main():
    module = build_module()
    choice_map = {
        'present': 'installed',
        'absent': 'deleted',
        'started': 'started'
    }
    params = module.params

    api_username = params['login']
    api_password = params['password']
    api_host = params['host']
    api_port = params['port']
    cluster_name = params['cluster_name']

    api_client = Ambari(api_host, port=api_port, username=api_username, password=api_password)

    if params["service"] is not None:
        components = params['components']
        config_path = params['config_path']
        component_hosts_count = params['count']
        extra_config = params['extra'] or None
        service = Mpack(params["service"], components, component_hosts_count,
                        config_path, extra_config, cluster_name, api_client, module)
        try:
            getattr(service, choice_map.get(params["state"]))()
        except Exception as exception:
            module.fail_json(msg=f"Cluster error : {exception}")
        module.exit_json(changed=service.changed,
                         msg=f"{service.service_name} is {service.status}",
                         meta=service.meta())
    else:
        module.fail_json(changed=False,
                            msg="No valid parameters combination was used: \"product\" is not set, exiting", meta=[])


if __name__ == "__main__":
    main()
