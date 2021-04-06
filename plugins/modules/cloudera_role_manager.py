#!/usr/bin/python3
# -*- coding: utf-8 -*-

from cm_client import (ApiConfig, ApiConfigList, ApiHost, ApiHostList, ApiCluster, ApiClusterList,
                       ApiServiceList, ApiService, ApiRoleList, ApiRole, ApiRolesToInclude, ApiRestartClusterArgs,
                       ApiRollingRestartClusterArgs, ApiCommand, ApiHostRef, ApiRoleNameList)
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.cloudera_manager import ClusterManager


########################################
ANSIBLE_METADATA = {
    "metadata_version": "1.0",
    "supported_by": "community",
    "status": ["preview"],
    "version": "1.1.0"
}


# Build module with params
def build_module():
    fields = {
        # Connection params
        "cm_login": {"required": True, "type": "str", "no_log": True},
        "cm_password": {"required": True, "type": "str", "no_log": True},
        "cm_host": {"required": True, "type": "str"},
        "cm_port": {"required": False, "type": "str", "default": "7180"},
        "cm_proto": {"required": False, "type": "str", "default": "http"},
        "api_version": {"required": False, "type": "str", "default": "18"},  # 18+
        "config_view": {"required": False, "type": "str"},  # Config size on "info": summary/full
        # Application params
        "cluster": {"required": False, "type": "str"},  # Clusters. If empty - operate with the only cluster or fail.
        "services": {"required": True, "type": "str"},  # Services to operate. If empty - operating with clusters.
        "roles": {"required": False, "type": "str", "default": "[]"},
        "roles_configs": {"required": False, "type": "str", "default": "[]"},
        "action": {
            '''
            info - Return roles
            roles-add - Add role
            roles-delete - Delete role

            '''
            "default": "info",
            "choices": [
                "info",
                "roles-add", "roles-delete",
                "roles-start", "roles-stop", "roles-restart", "roles-execute",
                "roles-get-config", "roles-set-config",
                "deploy-client-config"
            ],
            "type": "str"
        }
    }
    module = AnsibleModule(
        argument_spec=fields,
        mutually_exclusive=[],
        supports_check_mode=True
    )

    return module


########################################
class ClusterRoleManager(ClusterManager):

    # #################### Add/Delete ####################
    # Add roles
    @ClusterManager.Decorators.try_cm_api
    def roles_add(self, cluster_name, service_name, roles):
        roles_list = list()
        for role in roles:
            # If there are roles with the same parameters - we can't add new one
            if len(self._guess_role_name_by_parameters(role, service_name)) > 0:
                continue
            # Host
            host_id = self._get_host_id_by_hostname(role.get("host").strip())
            # Config
            config_list = list()
            for parameter in role.get("config", list()):
                config_list.append(ApiConfig(
                    name=parameter.get("name"),
                    value=parameter.get("value")
                ))
            # Prepare parameters and create ApiRole object
            temp_role_params = dict()
            temp_role_params.update({"host_ref": ApiHostRef(host_id=host_id)})
            temp_role_params.update({"type": role.get("type")})
            if config_list:
                temp_role_params.update({"config": ApiConfigList(config_list)})
            if role.get("name"):
                temp_role_params.update({"name": role.get("name")})
            temp_role = ApiRole(**temp_role_params)
            roles_list.append(temp_role)
        if roles_list:
            created_roles = self.cm_roles_api_client.create_roles(
                cluster_name=cluster_name,
                service_name=service_name,
                body=ApiRoleList(roles_list),
            )
            self.changed = True
            return {"roles": [role.to_dict() for role in created_roles.items]}
        else:
            return {"roles": list()}

    # Delete roles
    @ClusterManager.Decorators.try_cm_api
    def roles_delete(self, cluster_name, service_name, roles):
        roles_to_delete = list()
        for role in roles:
            # Calculating the role name
            assumed_role_names = list()
            if role.get("name"):
                assumed_role_names.append(role.get("name"))  # But how do you know the role name? Hm?
            else:
                assumed_role_names = self._guess_role_name_by_parameters(role, service_name)
            roles_to_delete += assumed_role_names
        deleted_roles = self.cm_roles_api_client.bulk_delete_roles(
            cluster_name,
            service_name,
            body=ApiRoleNameList(roles_to_delete)
        )
        if deleted_roles:
            self.changed = True
        return {"roles": [role.to_dict() for role in deleted_roles.items]}

    # #################### Start/Stop/Restart/Execute ####################
    # Start roles
    @ClusterManager.Decorators.try_cm_api
    def roles_start(self, cluster_name, service_name, roles):
        roles_to_start_list = list()
        already_started_roles = list()
        for role in roles:
            # Calculating the role name
            assumed_role_names = list()
            if role.get("name"):
                assumed_role_names.append(role.get("name"))  # But how do you know the role name? Hm?
            else:
                assumed_role_names = self._guess_role_name_by_parameters(role, service_name)
            for role in self.roles.items:
                if role.name in assumed_role_names:
                    if role.role_state != "STARTED":
                        roles_to_start_list.append(role.name)
                    else:
                        already_started_roles.append(role.name)
        # If all the supposed to start roles are already started - do nothing
        if len(already_started_roles) == len(roles):
            return {"commands": list()}
        start_parameters = dict(
            cluster_name=cluster_name,
            service_name=service_name
        )
        # If no roles match - do nothing
        if roles_to_start_list:
            start_parameters.update({"body": ApiRoleNameList(roles_to_start_list)})
        start_commands = self.cm_role_commands_api_client.start_command(**start_parameters)
        # Wait for all commands
        start_commands_waiting_line = list()
        for command in start_commands.items:
            waiting_command = self._wait_for_command_exec(command)
            start_commands_waiting_line.append(waiting_command)
            if waiting_command.success:
                self.changed = True
        return {"commands": [command.to_dict() for command in start_commands_waiting_line]}

    # Stop roles
    @ClusterManager.Decorators.try_cm_api
    def roles_stop(self, cluster_name, service_name, roles):
        roles_to_stop_list = list()
        already_stopped_roles = list()
        for role in roles:
            # Calculating the role name
            assumed_role_names = list()
            if role.get("name"):
                assumed_role_names.append(role.get("name"))  # But how do you know the role name? Hm?
            else:
                assumed_role_names = self._guess_role_name_by_parameters(role, service_name)
            for role in self.roles.items:
                if role.name in assumed_role_names:
                    if role.role_state != "STOPPED":
                        roles_to_stop_list.append(role.name)
                    else:
                        already_stopped_roles.append(role.name)
        # If all the supposed to stop roles are already stopped - do nothing
        if len(already_stopped_roles) == len(roles):
            return {"commands": list()}
        stop_parameters = dict(
            cluster_name=cluster_name,
            service_name=service_name
        )
        # If no roles match - do nothing
        if roles_to_stop_list:
            stop_parameters.update({"body": ApiRoleNameList(roles_to_stop_list)})
        # Wait for all commands
        stop_commands = self.cm_role_commands_api_client.stop_command(**stop_parameters)
        stop_commands_waiting_line = list()
        for command in stop_commands.items:
            waiting_command = self._wait_for_command_exec(command)
            stop_commands_waiting_line.append(waiting_command)
            if waiting_command.success:
                self.changed = True
        return {"commands": [command.to_dict() for command in stop_commands_waiting_line]}

    # Restart roles
    @ClusterManager.Decorators.try_cm_api
    def roles_restart(self, cluster_name, service_name, roles):
        roles_to_restart_list = list()
        for role in roles:
            # Calculating the role name
            assumed_role_names = list()
            if role.get("name"):
                assumed_role_names.append(role.get("name"))  # But how do you know the role name? Hm?
            else:
                assumed_role_names = self._guess_role_name_by_parameters(role, service_name)
            for role in self.roles.items:
                if role.name in assumed_role_names:
                    roles_to_restart_list.append(role.name)
        restart_parameters = dict(
            cluster_name=cluster_name,
            service_name=service_name
        )
        # If no roles match - do nothing
        if roles_to_restart_list:
            restart_parameters.update({"body": ApiRoleNameList(roles_to_restart_list)})
        # Wait for all commands
        restart_commands = self.cm_role_commands_api_client.restart_command(**restart_parameters)
        restart_commands_waiting_line = list()
        for command in restart_commands.items:
            waiting_command = self._wait_for_command_exec(command)
            restart_commands_waiting_line.append(waiting_command)
            if waiting_command.success:
                self.changed = True
        return {"commands": [command.to_dict() for command in restart_commands_waiting_line]}

    # Execute command
    @ClusterManager.Decorators.try_cm_api
    def roles_execute(self, cluster_name, service_name, roles):
        commands_to_wait_for = list()
        execute_commands_waiting_line = list()
        for role in roles:
            # Calculating the role name
            assumed_role_names = list()
            role_to_execute = None
            command_name = role.get("command_name")
            if role.get("name"):
                assumed_role_names.append(role.get("name"))  # But how do you know the role name? Hm?
            else:
                assumed_role_names = self._guess_role_name_by_parameters(role, service_name)
            for cluster_role in self.roles.items:
                if cluster_role.name in assumed_role_names:
                    role_to_execute = cluster_role.name
            execute_parameters = dict(
                cluster_name=cluster_name,
                service_name=service_name,
                command_name=command_name,
            )
            # If no roles match - do nothing
            if role_to_execute:
                execute_parameters.update({"body": ApiRoleNameList([role_to_execute])})
            # Wait for all commands
            execute_commands = self.cm_role_commands_api_client.role_command_by_name(**execute_parameters)
            commands_to_wait_for.append(execute_commands)
        for command_group in commands_to_wait_for:
            for command in command_group.items:
                waiting_command = self._wait_for_command_exec(command)
                execute_commands_waiting_line.append(waiting_command)
                if waiting_command.success:
                    self.changed = True
        return {"commands": [command.to_dict() for command in execute_commands_waiting_line]}

    # #################### Get/Put config ####################
    # Get role config
    def roles_get_config(self, cluster_name, service_name, roles, config_view=None):
        roles_configs = list()
        assumed_role_names = list()
        view = config_view or self.config_view
        for role in roles:
            if role.get("name"):
                assumed_role_names.append(role.get("name"))  # But how do you know the role name? Hm?
            else:
                assumed_role_names = self._guess_role_name_by_parameters(role, service_name)
            if len(assumed_role_names) == 0:
                continue
            else:
                for role_name in assumed_role_names:
                    role_config = self.cm_roles_api_client.read_role_config(
                        cluster_name=cluster_name,
                        role_name=role_name,
                        service_name=service_name,
                        view=view
                    )
                    roles_configs += [config.to_dict() for config in role_config.items]
        return {"roles_configs": roles_configs}

    # Set role config
    def roles_set_config(self, cluster_name, service_name, roles, roles_configs):
        updated_configs = list()
        assumed_role_names = list()
        # Multiple roles (i.e. multiple hosts)
        for role in roles:
            configs_to_upload = list()
            if role.get("name"):
                assumed_role_names.append(role.get("name"))  # But how do you know the role name? Hm?
            else:
                assumed_role_names = self._guess_role_name_by_parameters(role, service_name)
            if len(assumed_role_names) == 0:
                continue
            for name, value in roles_configs.items():
                configs_to_upload.append(
                    ApiConfig(
                        name=name,
                        value=value
                    )
                )
            for role_name in assumed_role_names:
                role_config_update = self.cm_roles_api_client.update_role_config(
                    cluster_name=cluster_name,
                    role_name=role_name,
                    service_name=service_name,
                    body=ApiConfigList(configs_to_upload)
                )
                updated_configs += [config.to_dict() for config in role_config_update.items]
            if updated_configs:
                self.changed = True

        return {"roles_configs": updated_configs, "role_names": assumed_role_names}

    # #################### Deploy client configuration ####################
    def deploy_client_config(self, cluster_name, service_name, roles):
        command = self.cm_service_api_client.deploy_client_config_command(
            cluster_name=cluster_name,
            service_name=service_name,
        )
        finished_command = self._wait_for_command_exec(command)
        if finished_command.success:
            self.changed = True
        return {"commands": [finished_command.to_dict()]}


########################################
def main():
    # Get ClusterManager object. All entities configs and initial validations must be prepared inside of it.
    module = build_module()
    cm_instance = ClusterRoleManager(module)

    # Get cluster name
    if cm_instance.cluster_name is not None:
        cluster_name = cm_instance.cluster_name  # If the cluster is set by a playbook - operating it
    else:
        cluster_name = cm_instance.clusters.items[0].name  # We can serve only one cluster at a time

    # Just get info
    if cm_instance.action == "info":
        # Get the config and go home
        cm_instance.update_configs()
        cm_instance.module.exit_json(
            changed=cm_instance.changed,
            msg="Cluster information gathered",
            meta=cm_instance.meta(),
        )

    # Do something with roles
    elif cm_instance.action in [
        "roles-add", "roles-delete",
        "roles-start", "roles-stop", "roles-restart", "roles-execute",
        "roles-get-config", "deploy-client-config"
    ]:
        # Execute command
        results = getattr(cm_instance, f"{cm_instance.action}".replace("-", "_"))(
            cluster_name,
            cm_instance.services_names[0],
            cm_instance.roles_to_manage
        )
        cm_instance.update_configs()
        # Get command object and evaluating it
        cm_instance.module.exit_json(
            changed=cm_instance.changed,
            msg="Changes performed successfully" if cm_instance.changed else "Cluster already in the state",
            results=results,
        )

    # Set roles config
    elif cm_instance.action in [
        "roles-set-config"
    ]:
        # Execute command
        results = getattr(cm_instance, f"{cm_instance.action}".replace("-", "_"))(
            cluster_name,
            cm_instance.services_names[0],
            cm_instance.roles_to_manage,
            cm_instance.roles_configs
        )
        cm_instance.update_configs()
        # Get command object and evaluating it
        cm_instance.module.exit_json(
            changed=cm_instance.changed,
            msg="Changes performed successfully" if cm_instance.changed else "Cluster already in the state",
            results=results,
        )

    else:
        # Just in case
        cm_instance.module.fail_json(
            msg=f"WARNING! {cm_instance.action} is not implemented"
        )


if __name__ == "__main__":
    main()
