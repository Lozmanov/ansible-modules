#!/usr/bin/python3
# -*- coding: utf-8 -*-

from cm_client import (ApiConfig, ApiConfigList, ApiHost, ApiHostList, ApiCluster, ApiClusterList,
                       ApiServiceList, ApiService, ApiRoleList, ApiRole, ApiRolesToInclude, ApiRestartClusterArgs,
                       ApiRollingRestartClusterArgs, ApiCommand)
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
        "services": {"required": False, "type": "str"},  # Services to operate. If empty - operating with clusters.
        "only_stale_services": {"required": False, "type": "bool", "default": False},
        "redeploy_client_configuration": {"required": False, "type": "bool", "default": False},
        "un_upgraded_only": {"required": False, "type": "bool", "default": False},
        "rolling_restart_roles_type": {
            '''
            all_roles - Only the slave roles of the selected rolling-restartable services.
            slaves_only - Only the non-slave roles of the selected rolling-restartable services.
            non_slaves_only - All roles of the selected rolling-restartable services.
            '''
            "required": False,
            "default": "slaves_only",
            "choices": ["slaves_only", "non_slaves_only", "all_roles"],
            "type": "str",
        },
        "action": {
            '''
            info - Just print cluster/service "config_view"-type info
            stop - Stop cluster/service
            start - Start cluster/service
            restart - Restart cluster/service straight
            rolling-restart - Restart cluster/service rolling
            '''
            "default": "info",
            "choices": ["stop", "start", "restart", "rolling-restart", "info"],
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
class ClusterStateManager(ClusterManager):
    # Stop all services in the cluster.
    @ClusterManager.Decorators.try_cm_api
    def stop_cluster(self, cluster_name):
        # If all services already stopped - do nothing
        if len([serv for serv in self.services.items if serv.service_state != "STOPPED"]) > 0:
            command = self.cm_cluster_api_client.stop_command(cluster_name=cluster_name)
            self.cluster_stop_command = self._wait_for_command_exec(command)
            self.changed = True
        else:
            self.cluster_stop_command = ApiCommand(
                name="Stop",
                active=False,
                success=True,
                result_message="All services already stopped."
            )

    # Start all services in the cluster.
    @ClusterManager.Decorators.try_cm_api
    def start_cluster(self, cluster_name):
        # If all services already started - do nothing
        if len([serv for serv in self.services.items if serv.service_state != "STARTED"]) > 0:
            command = self.cm_cluster_api_client.start_command(cluster_name=cluster_name)
            self.cluster_start_command = self._wait_for_command_exec(command)
            self.changed = True
        else:
            self.cluster_start_command = ApiCommand(
                name="Start",
                active=False,
                success=True,
                result_message="All services already started."
            )

    # Restart cluster
    @ClusterManager.Decorators.try_cm_api
    def restart_cluster(self, cluster_name):
        stale_services_count = len([serv for serv in self.services.items if serv.config_staleness_status == "STALE"])
        if self.only_stale_services and stale_services_count == 0:
            self.cluster_restart_command = ApiCommand(
                name="Restart",
                active=False,
                success=True,
                result_message="No stale configurations found."
            )
        else:
            restart_config = ApiRestartClusterArgs(
                restart_only_stale_services=self.only_stale_services,  # Restart services with stale configs only.
                redeploy_client_configuration=self.redeploy_client_configuration,  # Re-deploy client configuration.
                restart_service_names=self.services_names  # List of services to restart.
            )
            self.debug_command = [
                self.only_stale_services,  # Restart services with stale configs only.
                self.redeploy_client_configuration,  # Re-deploy client configuration.
                self.services_names  # List of services to restart.
            ]
            command = self.cm_cluster_api_client.restart_command(cluster_name=cluster_name, body=restart_config)
            self.cluster_restart_command = self._wait_for_command_exec(command)
            self.changed = True

    # Restart cluster in a rolling manner
    # !!!Enterprise license only!!!
    @ClusterManager.Decorators.try_cm_api
    def rolling_restart_cluster(self, cluster_name):
        # If all services is stopped - do nothing
        services_to_restart = list()
        if self.services_names:
            for serv in self.services.items:
                if serv.name in self.services_names and serv.service_state == "STARTED":
                    services_to_restart.append(serv)
        else:
            services_to_restart = [serv.name for serv in self.services.items]

        if len(services_to_restart) == 0:
            self.cluster_rolling_restart_command = ApiCommand(
                name="Rolling-restart",
                active=False,
                success=True,
                result_message="No services pointed or they are stopped."
            )
        else:
            restart_config = ApiRollingRestartClusterArgs(
                slave_batch_size=1,  # Default value
                sleep_seconds=0,  # Default value
                slave_fail_count_threshold=0,  # Default value
                # Restart roles with stale configs only.
                stale_configs_only=self.only_stale_services,
                # Restart roles that haven't been upgraded yet.
                un_upgraded_only=self.un_upgraded_only,
                # Re-deploy client configuration.
                redeploy_client_configuration=self.redeploy_client_configuration,
                # Role types to restart. Default is slave roles only.
                roles_to_include=getattr(ApiRolesToInclude(), f"{self.rolling_restart_roles_type}"),
                # List of services to restart.
                restart_service_names=self.services_names or [serv.name for serv in self.services.items]
            )
            command = self.cm_cluster_api_client.rolling_restart(cluster_name=cluster_name, body=restart_config)
            self.cluster_rolling_restart_command = self._wait_for_command_exec(command)
            self.changed = True


########################################
def main():
    # Get ClusterManager object. All entities configs and initial validations must be prepared inside of it.
    module = build_module()
    cm_instance = ClusterStateManager(module)

    # Just get info
    if cm_instance.action == "info":
        # Just get config and go home
        cm_instance.update_configs()
        cm_instance.module.exit_json(
            changed=cm_instance.changed,
            msg="Cluster information gathered",
            meta=cm_instance.meta(),
        )

    # Stop/start the entire cluster (one for a time)
    elif cm_instance.action in ["start", "stop", "restart", "rolling-restart"]:
        if cm_instance.cluster_name is not None:
            cluster_name = cm_instance.cluster_name  # If the cluster is set by a playbook - operating it
        else:
            cluster_name = cm_instance.clusters.items[0].name  # We can serve only one cluster at a time
        # Execute command
        getattr(cm_instance, f"{cm_instance.action}_cluster".replace("-", "_"))(cluster_name)
        cm_instance.update_configs()
        # Get command object and evaluating it
        executed_command = getattr(cm_instance, f"cluster_{cm_instance.action}_command".replace("-", "_"))
        if executed_command.success:
            cm_instance.module.exit_json(
                changed=cm_instance.changed,
                msg="Changes performed successfully" if cm_instance.changed else "Cluster already in the state",
                meta=cm_instance.meta(),
                command=executed_command.to_dict(),
            )
        else:
            cm_instance.module.fail_json(
                changed=cm_instance.changed,
                msg=f"ERROR! Command '{executed_command.name}' is unsuccessful: '{executed_command.result_message}'",
                meta=cm_instance.meta(),
                command=executed_command.to_dict(),
            )

    else:
        cm_instance.module.fail_json(
            msg=f"WARNING! {cm_instance.action} is not implemented"
        )


if __name__ == "__main__":
    main()
