#!/usr/bin/python3
# -*- coding: utf-8 -*-

import cm_client
from cm_client.rest import ApiException
from cm_client import (ApiConfig, ApiConfigList, ApiHost, ApiHostList, ApiCluster, ApiClusterList,
                       ApiServiceList, ApiService, ApiRoleList, ApiRole, ApiRolesToInclude, ApiRestartClusterArgs,
                       ApiRollingRestartClusterArgs, ApiCommand, ApiCommandList, ApiBulkCommandList)
from functools import wraps
import time
import json


########################################
class ClusterManager(object):
    def __init__(self, module):
        # Init client params to connect to ClusterManager
        self.module = module
        params = self.module.params
        api_url = f"{params['cm_proto']}://{params['cm_host']}:{params['cm_port']}/api/v{params['api_version']}"
        cm_client.configuration.username = params['cm_login']
        cm_client.configuration.password = params['cm_password']
        cm_client.configuration.host = api_url
        api_client = cm_client.ApiClient()
        self.name = params.get('cm_host')  # Initial "cluster name" is, kinda, it's hostname

        # API clients to make requests through them
        self.cm_resource_api_client = cm_client.ClouderaManagerResourceApi(api_client)
        self.cm_cluster_api_client = cm_client.ClustersResourceApi(api_client)
        self.cm_mgmt_roles_api_client = cm_client.MgmtRolesResourceApi(api_client)
        self.cm_roles_api_client = cm_client.RolesResourceApi(api_client)
        self.cm_role_commands_api_client = cm_client.RoleCommandsResourceApi(api_client)
        self.cm_hosts_api_client = cm_client.HostsResourceApi(api_client)
        self.cm_service_api_client = cm_client.ServicesResourceApi(api_client)
        self.command_resource_api_client = cm_client.CommandsResourceApi(api_client)

        # Parameters and actions
        self.action = params.get("action")
        self.cluster_name = params["cluster"].upper() if params.get("cluster") is not None else params.get("cluster")
        if params.get("services") is not None:
            self.services_names = [service.strip() for service in params.get("services").split(",")]
        else:
            self.services_names = list()
        self.config_view = params.get("config_view")
        self.only_stale_services = params.get("only_stale_services")
        self.redeploy_client_configuration = params.get("redeploy_client_configuration")
        if params.get("rolling_restart_roles_type") is not None:
            self.rolling_restart_roles_type = params.get("rolling_restart_roles_type").upper()
        else:
            self.rolling_restart_roles_type = params.get("rolling_restart_roles_type")
        self.un_upgraded_only = params.get("un_upgraded_only")
        self.config_parameters = json.loads(params.get("config_parameters", "{}"))
        self.config_parameter_name = params.get("config_parameter")
        self.config_parameter_value = params.get("config_value")
        self.roles_to_manage = json.loads(params.get("roles", "[]"))
        self.roles_configs = json.loads(params.get("roles_configs", "[]"))

        # By default a module status is "not changed"
        self.changed = False

        # Entities set and update
        self.config = ApiConfigList([ApiConfig()])
        self.read_config()
        self.clusters = ApiClusterList([ApiCluster()])
        self.read_clusters()
        # Cluster name validation
        if self.cluster_name is not None and self.cluster_name not in [cluster.name for cluster in self.clusters.items]:
            self.module.fail_json(msg=f"Cluster {self.cluster_name} is absent on server {self.name}")
        if self.cluster_name is None and len(self.clusters.items) > 1:
            available_clusters = ",".join([cluster.name for cluster in self.clusters.items])
            self.module.fail_json(msg=f"Which cluster should I manage? Available clusters: {available_clusters}")
        self.hosts = ApiHostList([ApiHost()])
        self.read_hosts()
        self.services = ApiServiceList([ApiService()])
        self.read_services()
        self.roles = ApiRoleList([ApiRole])
        self.read_roles()

        # Commands
        self.cluster_stop_command = ApiCommand()
        self.cluster_start_command = ApiCommand()
        self.cluster_restart_command = ApiCommand()
        self.cluster_rolling_restart_command = ApiCommand()
        self.parcels_refresh_command = ApiCommand()
        self.clusters_config_refresh_commands = ApiCommandList([ApiCommand()])
        self.updated_config_part = ApiConfigList([ApiConfig])

        # Variables
        self.command_wait_interval = 3

        # Debug
        self.debug_command = list()

    # try-except for all the API calls to the ClusterManager
    class Decorators(object):
        @classmethod
        def try_cm_api(cls, func):
            @wraps(func)
            def wrapper(*args):
                try:
                    return func(*args)
                except ApiException as e:
                    args[0].module.fail_json(msg=f"Cluster Manager error : {e}")
            return wrapper

    # FUNCTIONS
    # Get ApiCommand by it's id
    @Decorators.try_cm_api
    def _read_command(self, command_id):
        return self.command_resource_api_client.read_command(command_id=command_id)

    # Waits for a command to be done and returns an ApiCommand object.
    @Decorators.try_cm_api
    def _wait_for_command_exec(self, command):
        while self._read_command(int(command.id)).active:
            time.sleep(self.command_wait_interval)
        return self._read_command(int(command.id))

    # Refresh parcel repos
    @Decorators.try_cm_api
    def _refresh_parcel_repos(self):
        return self.cm_resource_api_client.refresh_parcel_repos()

    # Upload config
    @Decorators.try_cm_api
    def _upload_config(self, config_body):
        return self.cm_resource_api_client.update_config(body=config_body)

    @Decorators.try_cm_api
    def _refresh_clusters_config(self):
        return_commands = []
        for cluster in self.clusters.items:
            return_commands.append(self._wait_for_command_exec(self.cm_cluster_api_client.refresh(cluster.name)))
        return(return_commands)

    def _get_host_id_by_hostname(self, hostname):
        for host in self.hosts.items:
            if host.hostname == hostname:
                return host.host_id

    def _guess_role_name_by_parameters(self, role, service_name):
        host_id = self._get_host_id_by_hostname(role.get("host"))
        role_type = role.get("type")
        guessed_roles = list()
        for role in self.roles.items:
            if role.type == role_type and role.service_ref.service_name == service_name:
                if host_id:
                    if role.host_ref.host_id == host_id:
                        guessed_roles.append(role.name)
                else:
                    guessed_roles.append(role.name)
        return guessed_roles

    def _put_config(self, config_update_list):
        # Assemble new config
        config_body = []
        for name, value in config_update_list.items():
            prop_dict = ApiConfig(
                name=name,
                value=value
            )
            config_body.append(prop_dict)
        prepared_config_body = ApiConfigList(config_body)
        # Upload config
        self.updated_config_part = self._upload_config(prepared_config_body)
        # After config refresh we need to reload parcels or restart services
        if set(['parcel_repo_path', 'remote_parcel_repo_urls']) & set([k.lower() for k, v in config_update_list.items()]):
            # Refresh parcels repo
            self.parcels_refresh_command = self._wait_for_command_exec(self._refresh_parcel_repos())
            return ApiCommandList([self.parcels_refresh_command])
        else:
            # Refresh all clusters configs
            cluster_refresh_commands = self._refresh_clusters_config()
            # Adding ApiCommandList([ApiCommand]) object to self
            self.clusters_config_refresh_commands = ApiCommandList(cluster_refresh_commands)
            return self.clusters_config_refresh_commands

    # Here we go for the cluster config
    @Decorators.try_cm_api
    def read_config(self, config_view="full"):
        self.config = self.cm_resource_api_client.get_config(view=config_view)

    # Get clusters list
    @Decorators.try_cm_api
    def read_clusters(self, config_view="full", cluster_type="any"):
        self.clusters = self.cm_cluster_api_client.read_clusters(cluster_type=cluster_type, view=config_view)

    # Get hosts list
    @Decorators.try_cm_api
    def read_hosts(self, config_view="full"):
        self.hosts = self.cm_hosts_api_client.read_hosts(view=config_view)

    # Get services list for all the clusters
    @Decorators.try_cm_api
    def read_services(self, config_view="full"):
        services_list = list()
        for cluster in self.clusters.items:
            for serv in self.cm_service_api_client.read_services(cluster_name=cluster.name, view=config_view).items:
                services_list.append(serv)
        self.services = ApiServiceList(services_list)

    # Get roles list
    @Decorators.try_cm_api
    def read_roles(self, config_view="full"):
        roles = []
        # Get roles by cluster and service
        for cluster in self.clusters.items:
            for service in self.services.items:
                for role in self.cm_roles_api_client.read_roles(
                        cluster_name=cluster.name,
                        service_name=service.name,
                        filter="",  # Default
                        view=config_view
                        ).items:
                    roles.append(role)
        self.roles = ApiRoleList(roles)

    # Updates config in the short view. If you want full configs - set the request parameter "config_view" to the "full"
    def update_configs(self):
        self.read_config(self.config_view or "summary")
        self.read_clusters(self.config_view or "summary")
        self.read_hosts(self.config_view or "summary")
        self.read_services(self.config_view or "summary")
        self.read_roles(self.config_view or "summary")

    # Return info according to request parameters
    def meta(self):
        # Clusters and Hosts
        if self.cluster_name is not None:
            # Clusters filter by cluster
            clusters_content = [item.to_dict() for item in self.clusters.items
                                if item.name == self.cluster_name]
            # Hosts filter by cluster
            hosts_content = [item.to_dict() for item in self.hosts.items
                             if item.cluster_ref.cluster_name == self.cluster_name]
        else:
            clusters_content = self.clusters.to_dict().get("items")
            hosts_content = self.hosts.to_dict().get("items")
        # Services and Roles
        if self.services_names:
            # Services filter by cluster
            services_content = []
            for serv in self.services.items:
                if serv.cluster_ref is not None:
                    if serv.cluster_ref.cluster_name in [cluster.get("name") for cluster in clusters_content]\
                            and serv.name in self.services_names:
                        services_content.append(serv.to_dict())
            # Roles filter by service
            roles_content = []
            for role in self.roles.items:
                if role.service_ref.cluster_name in [cluster.get("name") for cluster in clusters_content]\
                        and role.service_ref.service_name in [serv.get("name") for serv in services_content]:
                    roles_content.append(role.to_dict())
                    break
        else:
            services_content = self.services.to_dict().get("items")
            roles_content = self.roles.to_dict().get("items")
        # Config refresh and parcels
        return {
            "cluster_name": self.name,
            "config": self.config.to_dict().get("items"),
            "clusters": clusters_content,
            "hosts": hosts_content,
            "services": services_content,
            "roles": roles_content,
        }

    def __repr__(self):
        return f'ClusterManager(name={self.name})'

    def __str__(self):
        return f"name: {self.name}"
