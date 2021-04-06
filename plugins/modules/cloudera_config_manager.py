#!/usr/bin/python3
# -*- coding: utf-8 -*-

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.cloudera_manager import ClusterManager


########################################
ANSIBLE_METADATA = {
    "metadata_version": "1.0",
    "supported_by": "community",
    "status": ["preview"],
    "version": "1.1.0"
}


########################################
def build_module():
    fields = {
        "cm_login": {"required": True, "type": "str", "no_log": True},
        "cm_password": {"required": True, "type": "str", "no_log": True},
        "cm_host": {"required": True, "type": "str"},
        "cm_port": {"required": False, "type": "str", "default": "7180"},
        "cm_proto": {"required": False, "type": "str", "default": "http"},
        "api_version": {"required": False, "type": "str", "default": "18"},
        "config_view": {"required": False, "type": "str"},  # Config size on "info": summary/full
        # Application params
        "cluster": {"required": False, "type": "str"},  # Clusters. If empty - operate with the only cluster or fail.
        "config_parameters": {"required": False, "type": "json", "default": "{}"},
        "config_parameter": {"required": False, "type": "str"},
        "config_value": {"required": False, "type": "str"},
        "action": {
            "default": "info",
            "choices": ['info', 'set', 'append', 'absent'],
            "type": 'str'
        }
    }
    module = AnsibleModule(
        argument_spec=fields,
        mutually_exclusive=[],
        supports_check_mode=True
    )

    return module


########################################
class ClusterConfigManager(ClusterManager):

    # Single param_name:param_values or json object in params
    def set_prop(self, action, param_name=None, param_values=None, params=None, override=False):
        # Converting formats of parameters list
        if params:
            new_params = params
        else:
            new_params = {param_name: param_values}

        config_params_to_update = dict()

        for param_name, param_value in new_params.items():
            if action == 'set':
                config_params_to_update.update({param_name: param_value})
            elif action == 'append':
                for config in self.config.items:
                    if config.name == param_name:
                        param_values_list = {val.strip() for val in param_value.split(",")}
                        config_params_to_update.update({
                            param_name: ",".join(
                                param_values_list.union({val.strip() for val in config.value.split(",")})
                            )
                        })
            elif action == 'absent':
                for config in self.config.items:
                    if config.name == param_name:
                        current_values = {val.strip() for val in config.value.split(",")}
                        param_values_list = {val.strip() for val in param_value.split(",")}
                        current_values.difference_update(param_values_list)
                        config_params_to_update.update({param_name: ",".join(current_values)})

        config_update_command = self._put_config(config_params_to_update)
        self.changed = True
        return config_update_command


########################################
def main():
    module = build_module()
    cm_config = ClusterConfigManager(module=module)

    if cm_config.action == "info":
        # Just get all info
        cm_config.update_configs()
        module.exit_json(
            changed=cm_config.changed,
            msg="Parameters information gathered",
            meta=cm_config.meta()
        )
    else:
        # # DEBUG
        # if cm_config.config_parameters is not None:
        #     module.fail_json(cm_config.config_parameters)
        # # /DEBUG
        # Execute command
        executed_command = cm_config.set_prop(
            action=cm_config.action,
            param_name=cm_config.config_parameter_name,
            param_values=cm_config.config_parameter_value,
            params=cm_config.config_parameters
        )
        cm_config.update_configs()
        if False not in [command.success for command in executed_command.items]:
            module.exit_json(
                changed=cm_config.changed,
                msg='Parameters {p_name} is in desired state: {p_action} {p_value}'.format(
                    p_name=cm_config.config_parameter_name,
                    p_value=cm_config.config_parameter_value,
                    p_action=cm_config.action,
                ),
                meta=cm_config.meta(),
                command=executed_command.to_dict(),
                parcel_refresh=cm_config.parcels_refresh_command.to_dict(),
                config_refresh=cm_config.clusters_config_refresh_commands.to_dict().get("items"),
                updated_config_part=cm_config.updated_config_part.to_dict().get("items"),
            )
        else:
            module.fail_json(
                changed=cm_config.changed,
                msg=f"ERROR! Command '{executed_command.name}' is unsuccessful: '{executed_command.result_message}'",
                meta=cm_config.meta(),
                command=executed_command.to_dict(),
                parcel_refresh=cm_config.parcels_refresh_command.to_dict(),
                config_refresh=cm_config.clusters_config_refresh_commands.to_dict().get("items"),
                updated_config_part=cm_config.updated_config_part.to_dict().get("items"),
            )


if __name__ == "__main__":
    main()
